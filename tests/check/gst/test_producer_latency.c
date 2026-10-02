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
#include <gst/base/gstbasesink.h>

/*
 * Producer latency propagation.
 *
 * A clock-synced interpipesink (sync=true) hands each buffer over only at
 * running time + latency + ts-offset - render-delay, so under passthrough-ts
 * and compensate-ts every buffer reaches the consumer that much behind its
 * running time. The consumer must learn about that delay through its own
 * LATENCY query, or every clock-synced element downstream treats the buffers
 * as late.
 *
 * The producer pipeline's latency is pinned with GstPipeline:latency so the
 * value is deterministic rather than whatever audiotestsrc reports. Every
 * consumer sink runs sync=true: GstBaseSink only answers "live" (and GstBin
 * only configures latency) when the sink syncs to the clock.
 *
 * The consumer's sink is configured with the upstream latency plus its own
 * processing deadline. interpipesrc answers an unbounded maximum, so the
 * deadline always fits; a bounded maximum equal to the minimum would make the
 * sink drop it.
 */

#define PRODUCER_LATENCY (500 * GST_MSECOND)

#define PRODUCER_DESC(sink_props) \
    "audiotestsrc is-live=true ! interpipesink name=sink async=false " \
    sink_props

#define CONSUMER_DESC(stream_sync) \
    "interpipesrc name=isrc listen-to=sink is-live=true format=time " \
    "stream-sync=" stream_sync " ! fakesink name=fsink sync=true " \
    "async=false signal-handoffs=true"

/* Buffers to see rendered before asserting a value that must not change once
 * data flows. */
#define SETTLE_BUFFERS 5

typedef struct
{
  GMutex lock;
  GCond cond;
  guint rendered;
} Rendered;

static Rendered *
rendered_new (void)
{
  Rendered *r = g_new0 (Rendered, 1);

  g_mutex_init (&r->lock);
  g_cond_init (&r->cond);
  return r;
}

static void
rendered_free (Rendered * r)
{
  g_cond_clear (&r->cond);
  g_mutex_clear (&r->lock);
  g_free (r);
}

static void
on_handoff (GstElement * fakesink, GstBuffer * buffer, GstPad * pad,
    gpointer user_data)
{
  Rendered *r = user_data;

  g_mutex_lock (&r->lock);
  r->rendered++;
  g_cond_signal (&r->cond);
  g_mutex_unlock (&r->lock);
}

static void
play (GstPipeline * pipeline)
{
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (pipeline), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (pipeline), NULL, NULL,
          GST_CLOCK_TIME_NONE));
}

static GstPipeline *
start_consumer (const gchar * description, Rendered * r)
{
  GstPipeline *pipeline;
  GstElement *fsink;
  GError *error = NULL;

  pipeline = GST_PIPELINE (gst_parse_launch (description, &error));
  fail_if (error, "%s", error ? error->message : "");

  fsink = gst_bin_get_by_name (GST_BIN (pipeline), "fsink");
  g_signal_connect (fsink, "handoff", G_CALLBACK (on_handoff), r);
  gst_object_unref (fsink);

  play (pipeline);
  return pipeline;
}

/* Bring up the producer and verify that its interpipesink really runs with
 * the pinned latency, so a failure further down points at the plug-in and not
 * at the test's own setup. */
static GstPipeline *
start_producer (const gchar * description, GstClockTime latency)
{
  GstPipeline *producer;
  GstElement *sink;
  GError *error = NULL;

  producer = GST_PIPELINE (gst_parse_launch (description, &error));
  fail_if (error, "%s", error ? error->message : "");
  g_object_set (producer, "latency", latency, NULL);

  play (producer);

  sink = gst_bin_get_by_name (GST_BIN (producer), "sink");
  fail_unless (sink != NULL);
  if (gst_base_sink_get_sync (GST_BASE_SINK (sink)))
    fail_unless_equals_uint64 (gst_base_sink_get_latency (GST_BASE_SINK (sink)),
        latency);
  gst_object_unref (sink);

  return producer;
}

static void
stop_pipeline (GstPipeline * pipeline)
{
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (pipeline), GST_STATE_NULL));
  gst_object_unref (pipeline);
}

static GstBaseSink *
consumer_sink (GstPipeline * consumer)
{
  GstElement *fsink = gst_bin_get_by_name (GST_BIN (consumer), "fsink");

  fail_unless (fsink != NULL);
  return GST_BASE_SINK (fsink);
}

/* The latency the consumer's sink is configured with when upstream reports
 * the given latency: upstream plus the sink's own processing deadline. */
static GstClockTime
expected_latency (GstPipeline * consumer, GstClockTime upstream)
{
  GstBaseSink *fsink = consumer_sink (consumer);
  GstClockTime deadline = gst_base_sink_get_processing_deadline (fsink);

  gst_object_unref (fsink);
  return upstream + deadline;
}

static GstClockTime
consumer_latency (GstPipeline * consumer)
{
  GstBaseSink *fsink = consumer_sink (consumer);
  GstClockTime latency = gst_base_sink_get_latency (fsink);

  gst_object_unref (fsink);
  return latency;
}

/* Poll until the consumer's configured latency reaches the expected value.
 * Returns the last value seen so the caller can report it on failure. */
static GstClockTime
wait_consumer_latency (GstPipeline * consumer, GstClockTime expected)
{
  GstClockTime latency = GST_CLOCK_TIME_NONE;
  gint i;

  for (i = 0; i < 50; i++) {
    latency = consumer_latency (consumer);
    if (latency == expected)
      break;
    g_usleep (100 * 1000);
  }

  return latency;
}

/* Wait until buffers flow through the consumer, so anything create() would
 * do on the first buffers after an attach has happened. */
static void
wait_rendered (Rendered * r, guint count)
{
  gint64 deadline = g_get_monotonic_time () + 10 * G_TIME_SPAN_SECOND;

  g_mutex_lock (&r->lock);
  while (r->rendered < count)
    if (!g_cond_wait_until (&r->cond, &r->lock, deadline))
      break;
  fail_unless (r->rendered >= count, "Only %u buffers rendered", r->rendered);
  g_mutex_unlock (&r->lock);
}

static void
assert_consumer_latency (GstPipeline * consumer, GstClockTime upstream)
{
  GstClockTime expected = expected_latency (consumer, upstream);
  GstClockTime latency = wait_consumer_latency (consumer, expected);

  fail_unless (latency == expected, "Consumer latency %" GST_TIME_FORMAT
      ", expected %" GST_TIME_FORMAT " (upstream %" GST_TIME_FORMAT ")",
      GST_TIME_ARGS (latency), GST_TIME_ARGS (expected),
      GST_TIME_ARGS (upstream));
}

/* Producer first: the consumer's LATENCY query already sees the producer's
 * latency when the consumer pipeline computes its own on the way to
 * PLAYING. */
static void
check_reported (const gchar * consumer_desc)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer (PRODUCER_DESC ("sync=true"), PRODUCER_LATENCY);
  consumer = start_consumer (consumer_desc, r);

  assert_consumer_latency (consumer, PRODUCER_LATENCY);
  wait_rendered (r, SETTLE_BUFFERS);
  assert_consumer_latency (consumer, PRODUCER_LATENCY);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_START_TEST (interpipe_producer_latency_reported_compensate_ts)
{
  check_reported (CONSUMER_DESC ("compensate-ts"));
}

GST_END_TEST;

GST_START_TEST (interpipe_producer_latency_reported_passthrough_ts)
{
  check_reported (CONSUMER_DESC ("passthrough-ts"));
}

GST_END_TEST;

/* Consumer first: its pipeline computes latency while the node does not exist
 * yet. Once the producer appears, interpipesrc must notice the new producer
 * latency and have the consumer pipeline recompute. */
GST_START_TEST (interpipe_producer_latency_recalculated)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;

  consumer = start_consumer (CONSUMER_DESC ("compensate-ts"), r);
  assert_consumer_latency (consumer, 0);

  producer = start_producer (PRODUCER_DESC ("sync=true"), PRODUCER_LATENCY);
  assert_consumer_latency (consumer, PRODUCER_LATENCY);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

/* The producer's latency changes while both pipelines run. The node tells its
 * listeners, and the consumer recomputes. */
GST_START_TEST (interpipe_producer_latency_change_followed)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer (PRODUCER_DESC ("sync=true"), PRODUCER_LATENCY);
  consumer = start_consumer (CONSUMER_DESC ("compensate-ts"), r);
  wait_rendered (r, SETTLE_BUFFERS);
  assert_consumer_latency (consumer, PRODUCER_LATENCY);

  g_object_set (producer, "latency", 2 * PRODUCER_LATENCY, NULL);
  fail_unless (gst_bin_recalculate_latency (GST_BIN (producer)));
  assert_consumer_latency (consumer, 2 * PRODUCER_LATENCY);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

/* ts-offset delays the hand-over and render-delay brings it forward; both
 * count, including when changed while running. */
GST_START_TEST (interpipe_producer_latency_ts_offset_render_delay)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;
  GstElement *sink;

  producer = start_producer (PRODUCER_DESC ("sync=true ts-offset=100000000 "
          "render-delay=30000000"), PRODUCER_LATENCY);
  consumer = start_consumer (CONSUMER_DESC ("compensate-ts"), r);
  assert_consumer_latency (consumer,
      PRODUCER_LATENCY + 100 * GST_MSECOND - 30 * GST_MSECOND);

  wait_rendered (r, SETTLE_BUFFERS);
  sink = gst_bin_get_by_name (GST_BIN (producer), "sink");
  g_object_set (sink, "ts-offset", (gint64) (200 * GST_MSECOND), NULL);
  gst_object_unref (sink);
  /* The consumer re-reads the delay on its next buffer. */
  assert_consumer_latency (consumer,
      PRODUCER_LATENCY + 200 * GST_MSECOND - 30 * GST_MSECOND);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

/* restart-ts re-stamps buffers on arrival, so the producer's delay is not
 * visible to the consumer and must not be added, including after buffers
 * flow. */
GST_START_TEST (interpipe_producer_latency_restart_ts_ignored)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer (PRODUCER_DESC ("sync=true"), PRODUCER_LATENCY);
  consumer = start_consumer (CONSUMER_DESC ("restart-ts"), r);

  wait_rendered (r, SETTLE_BUFFERS);
  assert_consumer_latency (consumer, 0);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

/* A producer sink that does not sync to the clock hands buffers over as soon
 * as they arrive, so there is no render latency to report. */
GST_START_TEST (interpipe_producer_latency_unsynced_ignored)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer (PRODUCER_DESC ("sync=false"), PRODUCER_LATENCY);
  consumer = start_consumer (CONSUMER_DESC ("compensate-ts"), r);

  wait_rendered (r, SETTLE_BUFFERS);
  assert_consumer_latency (consumer, 0);

  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

/* interpipesrc answers live with an unbounded maximum, unless its latency was
 * configured (appsrc applies max-latency together with min-latency), in which
 * case that bound is kept. */
GST_START_TEST (interpipe_producer_latency_max)
{
  Rendered *r = rendered_new ();
  GstPipeline *producer;
  GstPipeline *consumer;
  GstElement *isrc;
  GstQuery *query;
  gboolean live;
  GstClockTime min, max;

  producer = start_producer (PRODUCER_DESC ("sync=true"), PRODUCER_LATENCY);
  consumer = start_consumer (CONSUMER_DESC ("compensate-ts"), r);
  isrc = gst_bin_get_by_name (GST_BIN (consumer), "isrc");

  query = gst_query_new_latency ();
  fail_unless (gst_element_query (isrc, query));
  gst_query_parse_latency (query, &live, &min, &max);
  fail_unless (live);
  fail_unless_equals_uint64 (min, PRODUCER_LATENCY);
  fail_unless (!GST_CLOCK_TIME_IS_VALID (max), "Expected an unbounded "
      "maximum, got %" GST_TIME_FORMAT, GST_TIME_ARGS (max));
  gst_query_unref (query);

  g_object_set (isrc, "min-latency", (gint64) 0, "max-latency",
      (gint64) GST_SECOND, NULL);
  query = gst_query_new_latency ();
  fail_unless (gst_element_query (isrc, query));
  gst_query_parse_latency (query, &live, &min, &max);
  fail_unless_equals_uint64 (min, PRODUCER_LATENCY);
  fail_unless_equals_uint64 (max, GST_SECOND + PRODUCER_LATENCY);
  gst_query_unref (query);

  gst_object_unref (isrc);
  stop_pipeline (consumer);
  stop_pipeline (producer);
  rendered_free (r);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("producer_latency");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_producer_latency_reported_compensate_ts);
  tcase_add_test (tc, interpipe_producer_latency_reported_passthrough_ts);
  tcase_add_test (tc, interpipe_producer_latency_recalculated);
  tcase_add_test (tc, interpipe_producer_latency_change_followed);
  tcase_add_test (tc, interpipe_producer_latency_ts_offset_render_delay);
  tcase_add_test (tc, interpipe_producer_latency_restart_ts_ignored);
  tcase_add_test (tc, interpipe_producer_latency_unsynced_ignored);
  tcase_add_test (tc, interpipe_producer_latency_max);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
