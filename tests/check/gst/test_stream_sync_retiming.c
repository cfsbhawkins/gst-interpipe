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
 * Buffer and segment retiming across the interpipe boundary, per stream-sync
 * mode, against a producer whose segment does not start at 0.
 *
 * Encoders commonly offset their timeline (GstVideoEncoder adds 1000 hours so
 * DTS never goes negative): the segment starts at the offset and every
 * timestamp carries it, leaving running time unchanged. The producer here
 * reproduces that with a probe on the interpipesink pad.
 *
 * Expected behaviour on the consumer side:
 *
 * - passthrough-ts: buffers keep the producer's timestamps exactly, and the
 *   producer's segment is forwarded.
 * - compensate-ts: buffers are shifted by the difference between the two
 *   pipelines' base times, exactly, and the producer's segment is forwarded.
 * - restart-ts: buffers are re-stamped with the consumer's running time, so
 *   the producer's segment must not reach downstream. Every segment
 *   downstream starts at 0 and a clock-synced sink renders the buffers rather
 *   than clipping them as out of segment.
 *
 * The consumer is started before the producer so the producer's segment is
 * delivered to an already-running consumer, the way a segment re-sent
 * mid-stream would be. Buffers are matched across the boundary by their
 * offset, which videotestsrc sets to the frame number.
 */

#define TIMELINE_OFFSET (1000 * 3600 * GST_SECOND)

/* Enough frames that the producer's segment has been forwarded and buffers
 * keep flowing after it. */
#define FRAMES_WANTED 15

/* restart-ts stamps a buffer when it enters the consumer, so by the time the
 * sink sees it the consumer's running time has moved on by at most this
 * much (scheduling delay, generous for loaded machines). */
#define RESTAMP_TOLERANCE (GST_SECOND / 2)

typedef struct
{
  GMutex lock;
  GCond cond;
  /* Producer side: frame number -> PTS as it entered interpipesink. */
  GHashTable *produced;
  /* Consumer side, in arrival order. */
  GArray *offsets;
  GArray *pts;
  GArray *running_times;
  GArray *segment_starts;
  guint rendered;
  GstElement *consumer;
} Retiming;

static Retiming *
retiming_new (void)
{
  Retiming *r = g_new0 (Retiming, 1);

  g_mutex_init (&r->lock);
  g_cond_init (&r->cond);
  r->produced = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free,
      g_free);
  r->offsets = g_array_new (FALSE, FALSE, sizeof (guint64));
  r->pts = g_array_new (FALSE, FALSE, sizeof (GstClockTime));
  r->running_times = g_array_new (FALSE, FALSE, sizeof (GstClockTime));
  r->segment_starts = g_array_new (FALSE, FALSE, sizeof (guint64));

  return r;
}

static void
retiming_free (Retiming * r)
{
  g_hash_table_destroy (r->produced);
  g_array_free (r->offsets, TRUE);
  g_array_free (r->pts, TRUE);
  g_array_free (r->running_times, TRUE);
  g_array_free (r->segment_starts, TRUE);
  g_cond_clear (&r->cond);
  g_mutex_clear (&r->lock);
  g_free (r);
}

/* Shift the producer's segment and timestamps by TIMELINE_OFFSET, keeping
 * running time unchanged, and record each frame's resulting PTS. */
static GstPadProbeReturn
producer_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Retiming *r = user_data;

  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    GstBuffer *buffer = gst_buffer_make_writable (GST_PAD_PROBE_INFO_BUFFER
        (info));
    gint64 *offset;
    GstClockTime *pts;

    GST_BUFFER_PTS (buffer) += TIMELINE_OFFSET;
    if (GST_CLOCK_TIME_IS_VALID (GST_BUFFER_DTS (buffer)))
      GST_BUFFER_DTS (buffer) += TIMELINE_OFFSET;
    GST_PAD_PROBE_INFO_DATA (info) = buffer;

    offset = g_new (gint64, 1);
    *offset = GST_BUFFER_OFFSET (buffer);
    pts = g_new (GstClockTime, 1);
    *pts = GST_BUFFER_PTS (buffer);
    g_mutex_lock (&r->lock);
    g_hash_table_replace (r->produced, offset, pts);
    g_mutex_unlock (&r->lock);
  } else {
    GstEvent *event = GST_PAD_PROBE_INFO_EVENT (info);

    if (GST_EVENT_TYPE (event) == GST_EVENT_SEGMENT) {
      const GstSegment *segment;
      GstSegment shifted;
      GstEvent *replacement;

      gst_event_parse_segment (event, &segment);
      gst_segment_copy_into (segment, &shifted);
      shifted.start += TIMELINE_OFFSET;
      shifted.time += TIMELINE_OFFSET;
      shifted.position += TIMELINE_OFFSET;
      if (GST_CLOCK_TIME_IS_VALID (shifted.stop))
        shifted.stop += TIMELINE_OFFSET;

      replacement = gst_event_new_segment (&shifted);
      gst_event_set_seqnum (replacement, gst_event_get_seqnum (event));
      gst_event_unref (event);
      GST_PAD_PROBE_INFO_DATA (info) = replacement;
    }
  }

  return GST_PAD_PROBE_OK;
}

/* Record every segment and buffer as it reaches the consumer's sink, with the
 * consumer's running time at that moment. */
static GstPadProbeReturn
consumer_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Retiming *r = user_data;

  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER (info);
    guint64 offset = GST_BUFFER_OFFSET (buffer);
    GstClockTime pts = GST_BUFFER_PTS (buffer);
    GstClockTime running_time = GST_CLOCK_TIME_NONE;
    GstClock *clock;

    clock = gst_element_get_clock (r->consumer);
    if (clock) {
      running_time = gst_clock_get_time (clock) -
          gst_element_get_base_time (r->consumer);
      gst_object_unref (clock);
    }

    g_mutex_lock (&r->lock);
    g_array_append_val (r->offsets, offset);
    g_array_append_val (r->pts, pts);
    g_array_append_val (r->running_times, running_time);
    g_mutex_unlock (&r->lock);
  } else {
    GstEvent *event = GST_PAD_PROBE_INFO_EVENT (info);

    if (GST_EVENT_TYPE (event) == GST_EVENT_SEGMENT) {
      const GstSegment *segment;

      gst_event_parse_segment (event, &segment);
      g_mutex_lock (&r->lock);
      g_array_append_val (r->segment_starts, segment->start);
      g_mutex_unlock (&r->lock);
    }
  }

  return GST_PAD_PROBE_OK;
}

/* fakesink handoff: emitted only for buffers the sink actually renders, not
 * for buffers it clips as out of segment. */
static void
on_handoff (GstElement * fakesink, GstBuffer * buffer, GstPad * pad,
    gpointer user_data)
{
  Retiming *r = user_data;

  g_mutex_lock (&r->lock);
  r->rendered++;
  g_cond_signal (&r->cond);
  g_mutex_unlock (&r->lock);
}

static void
add_probe (GstElement * pipeline, const gchar * element, const gchar * pad,
    GstPadProbeCallback callback, Retiming * r)
{
  GstElement *e;
  GstPad *p;

  e = gst_bin_get_by_name (GST_BIN (pipeline), element);
  fail_unless (e != NULL);
  p = gst_element_get_static_pad (e, pad);
  fail_unless (p != NULL);
  gst_pad_add_probe (p, GST_PAD_PROBE_TYPE_BUFFER |
      GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, callback, r, NULL);
  gst_object_unref (p);
  gst_object_unref (e);
}

static void
play (GstElement * pipeline)
{
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (pipeline, GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (pipeline, NULL, NULL, GST_CLOCK_TIME_NONE));
}

/* Run a consumer in the given stream-sync mode against the offset producer
 * until FRAMES_WANTED buffers have been rendered (or 10 s pass). The
 * pipelines are returned so the caller can read their base times before
 * tearing them down. */
static Retiming *
run (const gchar * stream_sync, GstElement ** producer_out,
    GstElement ** consumer_out)
{
  Retiming *r = retiming_new ();
  GstElement *producer, *consumer, *fsink;
  gint64 deadline;
  gchar *desc;
  GError *error = NULL;

  desc = g_strdup_printf ("interpipesrc listen-to=retiming_node is-live=true "
      "format=time stream-sync=%s ! fakesink name=fsink sync=true "
      "async=false signal-handoffs=true", stream_sync);
  consumer = gst_parse_launch (desc, &error);
  g_free (desc);
  fail_if (error, "%s", error ? error->message : "");
  r->consumer = consumer;

  fsink = gst_bin_get_by_name (GST_BIN (consumer), "fsink");
  g_signal_connect (fsink, "handoff", G_CALLBACK (on_handoff), r);
  gst_object_unref (fsink);
  add_probe (consumer, "fsink", "sink", consumer_probe, r);

  producer = gst_parse_launch ("videotestsrc is-live=true ! "
      "video/x-raw,width=64,height=48,framerate=30/1 ! "
      "interpipesink name=retiming_node sync=false async=false", &error);
  fail_if (error, "%s", error ? error->message : "");
  add_probe (producer, "retiming_node", "sink", producer_probe, r);

  /* Consumer first: the producer's segment then reaches a running consumer. */
  play (consumer);
  play (producer);

  deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;
  g_mutex_lock (&r->lock);
  while (r->rendered < FRAMES_WANTED)
    if (!g_cond_wait_until (&r->cond, &r->lock, deadline))
      break;
  g_mutex_unlock (&r->lock);

  *producer_out = producer;
  *consumer_out = consumer;
  return r;
}

static void
stop (GstElement * producer, GstElement * consumer)
{
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (producer, GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (consumer, GST_STATE_NULL));
  gst_object_unref (producer);
  gst_object_unref (consumer);
}

static gboolean
saw_segment_start (Retiming * r, guint64 start)
{
  guint i;

  for (i = 0; i < r->segment_starts->len; i++)
    if (g_array_index (r->segment_starts, guint64, i) == start)
      return TRUE;
  return FALSE;
}

/* The PTS the producer gave the frame with this offset. */
static GstClockTime
produced_pts (Retiming * r, guint64 offset)
{
  gint64 key = offset;
  GstClockTime *pts = g_hash_table_lookup (r->produced, &key);

  fail_unless (pts != NULL, "Frame %" G_GUINT64_FORMAT " reached the consumer "
      "but was never produced", offset);
  return *pts;
}

GST_START_TEST (interpipe_retiming_passthrough_ts)
{
  GstElement *producer, *consumer;
  Retiming *r;
  guint i;

  r = run ("passthrough-ts", &producer, &consumer);

  g_mutex_lock (&r->lock);
  fail_unless (r->rendered >= FRAMES_WANTED, "Only %u buffers rendered",
      r->rendered);
  fail_unless (saw_segment_start (r, TIMELINE_OFFSET),
      "The producer's segment was not forwarded");
  for (i = 0; i < r->offsets->len; i++) {
    guint64 offset = g_array_index (r->offsets, guint64, i);

    fail_unless_equals_uint64 (g_array_index (r->pts, GstClockTime, i),
        produced_pts (r, offset));
  }
  g_mutex_unlock (&r->lock);

  stop (producer, consumer);
  retiming_free (r);
}

GST_END_TEST;

GST_START_TEST (interpipe_retiming_compensate_ts)
{
  GstElement *producer, *consumer;
  GstClockTime producer_base, consumer_base;
  Retiming *r;
  guint i;

  r = run ("compensate-ts", &producer, &consumer);
  producer_base = gst_element_get_base_time (producer);
  consumer_base = gst_element_get_base_time (consumer);

  g_mutex_lock (&r->lock);
  fail_unless (r->rendered >= FRAMES_WANTED, "Only %u buffers rendered",
      r->rendered);
  fail_unless (saw_segment_start (r, TIMELINE_OFFSET),
      "The producer's segment was not forwarded");
  for (i = 0; i < r->offsets->len; i++) {
    guint64 offset = g_array_index (r->offsets, guint64, i);
    GstClockTime in = produced_pts (r, offset);
    GstClockTime out = g_array_index (r->pts, GstClockTime, i);

    /* Same absolute clock time, expressed against the consumer's base. */
    fail_unless_equals_uint64 (out + consumer_base, in + producer_base);
  }
  g_mutex_unlock (&r->lock);

  stop (producer, consumer);
  retiming_free (r);
}

GST_END_TEST;

GST_START_TEST (interpipe_retiming_restart_ts)
{
  GstElement *producer, *consumer;
  Retiming *r;
  guint i;

  r = run ("restart-ts", &producer, &consumer);

  g_mutex_lock (&r->lock);
  /* The sink renders only buffers inside the segment, so this fails if the
   * producer's segment got through and clipped the re-stamped buffers. */
  fail_unless (r->rendered >= FRAMES_WANTED, "Only %u buffers rendered, the "
      "rest were clipped as out of segment", r->rendered);
  fail_unless (r->segment_starts->len > 0, "No segment reached the sink");
  for (i = 0; i < r->segment_starts->len; i++)
    fail_unless_equals_uint64 (g_array_index (r->segment_starts, guint64, i),
        0);

  for (i = 0; i < r->offsets->len; i++) {
    GstClockTime pts = g_array_index (r->pts, GstClockTime, i);
    GstClockTime running_time =
        g_array_index (r->running_times, GstClockTime, i);

    /* Stamped with the consumer's running time on entry, shortly before the
     * sink sees it, and none of the producer's timeline carried over. */
    fail_unless (GST_CLOCK_TIME_IS_VALID (pts));
    fail_unless (pts <= running_time, "PTS %" GST_TIME_FORMAT " is ahead of "
        "the consumer's running time %" GST_TIME_FORMAT, GST_TIME_ARGS (pts),
        GST_TIME_ARGS (running_time));
    fail_unless (running_time - pts <= RESTAMP_TOLERANCE, "PTS %"
        GST_TIME_FORMAT " lags the consumer's running time %" GST_TIME_FORMAT,
        GST_TIME_ARGS (pts), GST_TIME_ARGS (running_time));
  }
  g_mutex_unlock (&r->lock);

  stop (producer, consumer);
  retiming_free (r);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("stream_sync_retiming");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_retiming_passthrough_ts);
  tcase_add_test (tc, interpipe_retiming_compensate_ts);
  tcase_add_test (tc, interpipe_retiming_restart_ts);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
