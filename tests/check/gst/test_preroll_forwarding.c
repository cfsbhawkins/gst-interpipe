/* GStreamer
 * Copyright (C) 2026 Brian Hawkins <brian@prodjekt.co>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/check/gstcheck.h>

/*
 * How interpipesink hands over the buffer its base sink prerolls.
 *
 * GstBaseSink prerolls a buffer before waiting for its clock time and then
 * renders that same buffer once the time comes. Listeners must get it exactly
 * once, and at the on-time render whenever the producer is going to PLAYING,
 * when its base time is valid (compensate-ts depends on that). Only a
 * producer that stays paused hands the preroll buffer over at preroll.
 *
 * The producer is a non-live videotestsrc into a clock-synced interpipesink.
 * Frames are identified by their offset, which videotestsrc sets to the frame
 * number. The consumer records each frame it receives and the clock time it
 * arrived.
 */

#define FRAMERATE 30

#define PRODUCER_DESC \
    "videotestsrc ! video/x-raw,width=64,height=48,framerate=30/1 ! " \
    "interpipesink name=node sync=true"

#define CONSUMER_DESC(stream_sync) \
    "interpipesrc listen-to=node is-live=true format=time stream-sync=" \
    stream_sync " ! fakesink name=fsink sync=false async=false"

typedef struct
{
  GMutex lock;
  GCond cond;
  GstElement *consumer;
  /* Producer side: frame number -> PTS as it entered interpipesink. */
  GHashTable *produced;
  /* Consumer side, in arrival order. */
  GArray *offsets;
  GArray *arrivals;
} Frames;

static Frames *
frames_new (void)
{
  Frames *f = g_new0 (Frames, 1);

  g_mutex_init (&f->lock);
  g_cond_init (&f->cond);
  f->produced = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free,
      g_free);
  f->offsets = g_array_new (FALSE, FALSE, sizeof (guint64));
  f->arrivals = g_array_new (FALSE, FALSE, sizeof (GstClockTime));
  return f;
}

static void
frames_free (Frames * f)
{
  g_hash_table_destroy (f->produced);
  g_array_free (f->offsets, TRUE);
  g_array_free (f->arrivals, TRUE);
  g_cond_clear (&f->cond);
  g_mutex_clear (&f->lock);
  g_free (f);
}

static void
frames_clear (Frames * f)
{
  g_mutex_lock (&f->lock);
  g_array_set_size (f->offsets, 0);
  g_array_set_size (f->arrivals, 0);
  g_mutex_unlock (&f->lock);
}

static GstPadProbeReturn
producer_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Frames *f = user_data;
  GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER (info);
  gint64 *offset = g_new (gint64, 1);
  GstClockTime *pts = g_new (GstClockTime, 1);

  *offset = GST_BUFFER_OFFSET (buffer);
  *pts = GST_BUFFER_PTS (buffer);
  g_mutex_lock (&f->lock);
  g_hash_table_replace (f->produced, offset, pts);
  g_mutex_unlock (&f->lock);

  return GST_PAD_PROBE_OK;
}

static GstPadProbeReturn
consumer_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Frames *f = user_data;
  guint64 offset = GST_BUFFER_OFFSET (GST_PAD_PROBE_INFO_BUFFER (info));
  GstClock *clock = gst_element_get_clock (f->consumer);
  GstClockTime arrival = GST_CLOCK_TIME_NONE;

  if (clock) {
    arrival = gst_clock_get_time (clock);
    gst_object_unref (clock);
  }

  g_mutex_lock (&f->lock);
  g_array_append_val (f->offsets, offset);
  g_array_append_val (f->arrivals, arrival);
  g_cond_signal (&f->cond);
  g_mutex_unlock (&f->lock);

  return GST_PAD_PROBE_OK;
}

static void
add_probe (GstElement * pipeline, const gchar * element,
    GstPadProbeCallback callback, Frames * f)
{
  GstElement *e = gst_bin_get_by_name (GST_BIN (pipeline), element);
  GstPad *pad;

  fail_unless (e != NULL);
  pad = gst_element_get_static_pad (e, "sink");
  gst_pad_add_probe (pad, GST_PAD_PROBE_TYPE_BUFFER, callback, f, NULL);
  gst_object_unref (pad);
  gst_object_unref (e);
}

static void
set_state (GstElement * pipeline, GstState state)
{
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipeline,
          state));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_get_state (pipeline, NULL,
          NULL, GST_CLOCK_TIME_NONE));
}

/* Wait until the consumer has received at least count frames. */
static void
wait_frames (Frames * f, guint count)
{
  gint64 deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;

  g_mutex_lock (&f->lock);
  while (f->offsets->len < count)
    if (!g_cond_wait_until (&f->cond, &f->lock, deadline))
      break;
  fail_unless (f->offsets->len >= count, "Only %u frames received",
      f->offsets->len);
  g_mutex_unlock (&f->lock);
}

/* Bring up a consumer in the given mode, then the producer. */
static Frames *
start (const gchar * stream_sync, GstElement ** producer,
    GstElement ** consumer)
{
  Frames *f = frames_new ();
  gchar *desc = g_strdup_printf (CONSUMER_DESC ("%s"), stream_sync);
  GError *error = NULL;

  *consumer = gst_parse_launch (desc, &error);
  g_free (desc);
  fail_if (error, "%s", error ? error->message : "");
  f->consumer = *consumer;
  add_probe (*consumer, "fsink", consumer_probe, f);

  *producer = gst_parse_launch (PRODUCER_DESC, &error);
  fail_if (error, "%s", error ? error->message : "");
  add_probe (*producer, "node", producer_probe, f);

  set_state (*consumer, GST_STATE_PLAYING);
  return f;
}

static void
stop (Frames * f, GstElement * producer, GstElement * consumer)
{
  set_state (producer, GST_STATE_NULL);
  set_state (consumer, GST_STATE_NULL);
  gst_object_unref (producer);
  gst_object_unref (consumer);
  frames_free (f);
}

/* Every frame arrived once, in order, starting at first_offset, and none
 * before the producer's clock time for it. segment_start is the start of the
 * producer's segment (the seek position), so a frame's running time is its
 * PTS minus that. Called with f->lock held. */
static void
check_frames (Frames * f, guint64 first_offset, GstClockTime producer_base,
    GstClockTime segment_start)
{
  guint i;

  fail_unless_equals_uint64 (g_array_index (f->offsets, guint64, 0),
      first_offset);
  for (i = 0; i < f->offsets->len; i++) {
    guint64 offset = g_array_index (f->offsets, guint64, i);
    GstClockTime arrival = g_array_index (f->arrivals, GstClockTime, i);
    gint64 key = offset;
    GstClockTime *pts = g_hash_table_lookup (f->produced, &key);

    if (i > 0)
      fail_unless (offset > g_array_index (f->offsets, guint64, i - 1),
          "Frame %" G_GUINT64_FORMAT " after frame %" G_GUINT64_FORMAT,
          offset, g_array_index (f->offsets, guint64, i - 1));
    fail_unless (pts != NULL);
    fail_unless (*pts >= segment_start);
    fail_unless (arrival >= producer_base + *pts - segment_start, "Frame %"
        G_GUINT64_FORMAT " arrived %" GST_TIME_FORMAT " before its clock time",
        offset, GST_TIME_ARGS (producer_base + *pts - segment_start - arrival));
  }
}

/* The first frame is handed over at render, on time and with the producer's
 * base time valid, so compensate-ts delivers it rather than dropping it. */
GST_START_TEST (interpipe_preroll_first_frame_compensate_ts)
{
  GstElement *producer, *consumer;
  Frames *f;

  f = start ("compensate-ts", &producer, &consumer);
  set_state (producer, GST_STATE_PLAYING);
  wait_frames (f, 10);

  g_mutex_lock (&f->lock);
  check_frames (f, 0, gst_element_get_base_time (producer), 0);
  g_mutex_unlock (&f->lock);

  stop (f, producer, consumer);
}

GST_END_TEST;

/* After a flushing seek the producer prerolls again. The first frame after
 * the seek arrives once, in order, and not ahead of its clock time. */
GST_START_TEST (interpipe_preroll_after_flushing_seek)
{
  GstElement *producer, *consumer;
  Frames *f;

  f = start ("compensate-ts", &producer, &consumer);
  set_state (producer, GST_STATE_PLAYING);
  wait_frames (f, 5);

  fail_unless (gst_element_seek_simple (producer, GST_FORMAT_TIME,
          GST_SEEK_FLAG_FLUSH | GST_SEEK_FLAG_ACCURATE, 10 * GST_SECOND));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_get_state (producer, NULL,
          NULL, GST_CLOCK_TIME_NONE));
  /* Frames from before the seek may still be in flight; start counting at
   * the first one from after it. */
  g_usleep (200 * 1000);
  frames_clear (f);
  wait_frames (f, 10);

  g_mutex_lock (&f->lock);
  fail_unless (g_array_index (f->offsets, guint64, 0) >= 10 * FRAMERATE,
      "Frame %" G_GUINT64_FORMAT " is from before the seek",
      g_array_index (f->offsets, guint64, 0));
  check_frames (f, g_array_index (f->offsets, guint64, 0),
      gst_element_get_base_time (producer), 10 * GST_SECOND);
  g_mutex_unlock (&f->lock);

  stop (f, producer, consumer);
}

GST_END_TEST;

/* A producer that stays paused never renders, so it hands its preroll frame
 * over at preroll. Resuming renders that frame, which must not be handed over
 * a second time. */
GST_START_TEST (interpipe_preroll_paused_producer)
{
  GstElement *producer, *consumer;
  Frames *f;
  guint i, zeros = 0;

  f = start ("passthrough-ts", &producer, &consumer);
  set_state (producer, GST_STATE_PAUSED);
  wait_frames (f, 1);

  g_mutex_lock (&f->lock);
  fail_unless_equals_uint64 (g_array_index (f->offsets, guint64, 0), 0);
  fail_unless_equals_int (f->offsets->len, 1);
  g_mutex_unlock (&f->lock);

  set_state (producer, GST_STATE_PLAYING);
  wait_frames (f, 10);

  g_mutex_lock (&f->lock);
  for (i = 0; i < f->offsets->len; i++)
    if (g_array_index (f->offsets, guint64, i) == 0)
      zeros++;
  fail_unless_equals_int (zeros, 1);
  for (i = 1; i < f->offsets->len; i++)
    fail_unless (g_array_index (f->offsets, guint64, i) >
        g_array_index (f->offsets, guint64, i - 1));
  g_mutex_unlock (&f->lock);

  stop (f, producer, consumer);
}

GST_END_TEST;

/* A consumer that attaches while the producer is paused missed the preroll
 * hand-over. When the producer resumes and renders that frame, the late
 * consumer must get it, and the one that already has it must not get it
 * again. The producer makes a single frame, so the render is the late
 * consumer's only chance. */
GST_START_TEST (interpipe_preroll_listener_attached_while_paused)
{
  GstElement *producer, *consumer, *late_consumer;
  Frames *f, *late;
  GError *error = NULL;

  f = frames_new ();
  late = frames_new ();

  consumer = gst_parse_launch (CONSUMER_DESC ("passthrough-ts"), &error);
  fail_if (error, "%s", error ? error->message : "");
  f->consumer = consumer;
  add_probe (consumer, "fsink", consumer_probe, f);

  producer = gst_parse_launch ("videotestsrc num-buffers=1 ! "
      "video/x-raw,width=64,height=48,framerate=30/1 ! "
      "interpipesink name=node sync=true", &error);
  fail_if (error, "%s", error ? error->message : "");
  add_probe (producer, "node", producer_probe, f);

  set_state (consumer, GST_STATE_PLAYING);
  set_state (producer, GST_STATE_PAUSED);
  wait_frames (f, 1);

  late_consumer = gst_parse_launch (CONSUMER_DESC ("passthrough-ts"), &error);
  fail_if (error, "%s", error ? error->message : "");
  late->consumer = late_consumer;
  add_probe (late_consumer, "fsink", consumer_probe, late);
  set_state (late_consumer, GST_STATE_PLAYING);

  set_state (producer, GST_STATE_PLAYING);
  wait_frames (late, 1);
  /* Give a duplicate to the first consumer time to show up. */
  g_usleep (200 * 1000);

  g_mutex_lock (&late->lock);
  fail_unless_equals_int (late->offsets->len, 1);
  fail_unless_equals_uint64 (g_array_index (late->offsets, guint64, 0), 0);
  g_mutex_unlock (&late->lock);
  g_mutex_lock (&f->lock);
  fail_unless_equals_int (f->offsets->len, 1);
  g_mutex_unlock (&f->lock);

  set_state (producer, GST_STATE_NULL);
  set_state (late_consumer, GST_STATE_NULL);
  set_state (consumer, GST_STATE_NULL);
  gst_object_unref (producer);
  gst_object_unref (late_consumer);
  gst_object_unref (consumer);
  frames_free (late);
  frames_free (f);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("preroll_forwarding");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_preroll_first_frame_compensate_ts);
  tcase_add_test (tc, interpipe_preroll_after_flushing_seek);
  tcase_add_test (tc, interpipe_preroll_paused_producer);
  tcase_add_test (tc, interpipe_preroll_listener_attached_while_paused);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
