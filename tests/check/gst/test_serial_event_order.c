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
#include <gst/check/gstharness.h>

/*
 * Serialized events keep their place between buffers across the interpipe
 * boundary, also while buffers are queued in the consumer.
 *
 * The producer is an interpipesink driven by a GstHarness, so the test thread
 * pushes buffers and events in an exact order. The consumer's streaming
 * thread is held on frame 10 while the producer pushes frame 11, a
 * non-flushing segment update starting at 12 s, and frame 12, so all three
 * are queued in the consumer when it resumes. Downstream must see them in
 * that order: if the segment overtook frame 11, the sink would clip frame 11
 * as out of segment.
 */

#define NODE_NAME "order_node"
#define HOLD_AT 10
#define SEGMENT_UPDATE (12 * GST_SECOND)

#define CONSUMER_DESC \
    "interpipesrc name=isrc listen-to=" NODE_NAME " is-live=true format=time " \
    "stream-sync=passthrough-ts ! fakesink name=fsink sync=false " \
    "async=false signal-handoffs=true"

/* What reached the consumer's sink, in order: a buffer's frame number, or a
 * segment (SEGMENT_MARK + its start in seconds). */
#define SEGMENT_MARK 1000000

typedef struct
{
  GMutex lock;
  GCond cond;
  GArray *order;
  GArray *rendered;
  gboolean holding;
  gboolean released;
} Order;

static Order *
order_new (void)
{
  Order *o = g_new0 (Order, 1);

  g_mutex_init (&o->lock);
  g_cond_init (&o->cond);
  o->order = g_array_new (FALSE, FALSE, sizeof (guint64));
  o->rendered = g_array_new (FALSE, FALSE, sizeof (guint64));
  return o;
}

static void
order_free (Order * o)
{
  g_array_free (o->order, TRUE);
  g_array_free (o->rendered, TRUE);
  g_cond_clear (&o->cond);
  g_mutex_clear (&o->lock);
  g_free (o);
}

/* On the consumer's source pad: hold the streaming thread on frame HOLD_AT
 * until the test releases it. */
static GstPadProbeReturn
hold_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Order *o = user_data;
  gint64 deadline;

  if (GST_BUFFER_OFFSET (GST_PAD_PROBE_INFO_BUFFER (info)) != HOLD_AT)
    return GST_PAD_PROBE_OK;

  deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;
  g_mutex_lock (&o->lock);
  o->holding = TRUE;
  g_cond_broadcast (&o->cond);
  while (!o->released)
    if (!g_cond_wait_until (&o->cond, &o->lock, deadline))
      break;
  g_mutex_unlock (&o->lock);

  return GST_PAD_PROBE_REMOVE;
}

static GstPadProbeReturn
record_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Order *o = user_data;
  guint64 item;

  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    item = GST_BUFFER_OFFSET (GST_PAD_PROBE_INFO_BUFFER (info));
  } else {
    GstEvent *event = GST_PAD_PROBE_INFO_EVENT (info);
    const GstSegment *segment;

    if (GST_EVENT_TYPE (event) != GST_EVENT_SEGMENT)
      return GST_PAD_PROBE_OK;
    gst_event_parse_segment (event, &segment);
    item = SEGMENT_MARK + segment->start / GST_SECOND;
  }

  g_mutex_lock (&o->lock);
  g_array_append_val (o->order, item);
  g_cond_broadcast (&o->cond);
  g_mutex_unlock (&o->lock);

  return GST_PAD_PROBE_OK;
}

/* fakesink handoff: only for buffers it renders, not ones it clips. */
static void
on_handoff (GstElement * fakesink, GstBuffer * buffer, GstPad * pad,
    gpointer user_data)
{
  Order *o = user_data;
  guint64 offset = GST_BUFFER_OFFSET (buffer);

  g_mutex_lock (&o->lock);
  g_array_append_val (o->rendered, offset);
  g_cond_broadcast (&o->cond);
  g_mutex_unlock (&o->lock);
}

static void
push_frame (GstHarness * h, guint64 n)
{
  GstBuffer *buffer = gst_harness_create_buffer (h, 64 * 48 * 3 / 2);

  GST_BUFFER_PTS (buffer) = n * GST_SECOND;
  GST_BUFFER_DURATION (buffer) = GST_SECOND;
  GST_BUFFER_OFFSET (buffer) = n;
  GST_BUFFER_OFFSET_END (buffer) = n + 1;
  fail_unless_equals_int (gst_harness_push (h, buffer), GST_FLOW_OK);
}

static gint
index_of (GArray * array, guint64 item)
{
  guint i;

  for (i = 0; i < array->len; i++)
    if (g_array_index (array, guint64, i) == item)
      return i;
  return -1;
}

GST_START_TEST (interpipe_serial_event_midstream_order)
{
  GstElement *consumer, *node, *element;
  GstHarness *h;
  GstPad *pad;
  GstSegment segment;
  Order *o;
  gint64 deadline;
  gint b11, seg, b12;
  guint64 n;
  GError *error = NULL;

  o = order_new ();

  consumer = gst_parse_launch (CONSUMER_DESC, &error);
  fail_if (error, "%s", error ? error->message : "");
  element = gst_bin_get_by_name (GST_BIN (consumer), "isrc");
  pad = gst_element_get_static_pad (element, "src");
  gst_pad_add_probe (pad, GST_PAD_PROBE_TYPE_BUFFER, hold_probe, o, NULL);
  gst_object_unref (pad);
  gst_object_unref (element);
  element = gst_bin_get_by_name (GST_BIN (consumer), "fsink");
  g_signal_connect (element, "handoff", G_CALLBACK (on_handoff), o);
  pad = gst_element_get_static_pad (element, "sink");
  gst_pad_add_probe (pad, GST_PAD_PROBE_TYPE_BUFFER |
      GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, record_probe, o, NULL);
  gst_object_unref (pad);
  gst_object_unref (element);

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (consumer, GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_get_state (consumer, NULL,
          NULL, GST_CLOCK_TIME_NONE));

  node = gst_object_ref_sink (gst_element_factory_make ("interpipesink",
          NODE_NAME));
  fail_unless (node != NULL);
  g_object_set (node, "sync", FALSE, NULL);
  h = gst_harness_new_with_element (node, "sink", NULL);
  gst_harness_set_src_caps_str (h,
      "video/x-raw,format=I420,width=64,height=48,framerate=1/1");

  for (n = 0; n <= HOLD_AT; n++)
    push_frame (h, n);

  /* Wait for the consumer to be held on frame HOLD_AT, then queue frame 11,
   * the segment update and frame 12 behind it. */
  deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;
  g_mutex_lock (&o->lock);
  while (!o->holding)
    if (!g_cond_wait_until (&o->cond, &o->lock, deadline))
      break;
  fail_unless (o->holding, "The consumer never reached frame %d", HOLD_AT);
  g_mutex_unlock (&o->lock);

  push_frame (h, HOLD_AT + 1);
  gst_segment_init (&segment, GST_FORMAT_TIME);
  segment.start = SEGMENT_UPDATE;
  segment.time = SEGMENT_UPDATE;
  segment.position = SEGMENT_UPDATE;
  fail_unless (gst_harness_push_event (h, gst_event_new_segment (&segment)));
  push_frame (h, HOLD_AT + 2);

  g_mutex_lock (&o->lock);
  o->released = TRUE;
  g_cond_broadcast (&o->cond);
  deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;
  while (index_of (o->order, HOLD_AT + 2) < 0)
    if (!g_cond_wait_until (&o->cond, &o->lock, deadline))
      break;
  g_mutex_unlock (&o->lock);
  /* Let the handoff for the last frame land. */
  g_usleep (100 * 1000);

  g_mutex_lock (&o->lock);
  b11 = index_of (o->order, HOLD_AT + 1);
  seg = index_of (o->order, SEGMENT_MARK + SEGMENT_UPDATE / GST_SECOND);
  b12 = index_of (o->order, HOLD_AT + 2);
  fail_unless (b11 >= 0 && seg >= 0 && b12 >= 0,
      "Missing frame 11 (%d), the segment update (%d) or frame 12 (%d)", b11,
      seg, b12);
  fail_unless (b11 < seg && seg < b12, "Expected frame 11, segment, frame 12; "
      "got them at positions %d, %d, %d", b11, seg, b12);
  fail_unless (index_of (o->rendered, HOLD_AT + 1) >= 0,
      "Frame 11 was clipped instead of rendered");
  fail_unless (index_of (o->rendered, HOLD_AT + 2) >= 0,
      "Frame 12 was not rendered");
  g_mutex_unlock (&o->lock);

  gst_harness_teardown (h);
  gst_object_unref (node);
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (consumer, GST_STATE_NULL));
  gst_object_unref (consumer);
  order_free (o);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("serial_event_order");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_serial_event_midstream_order);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
