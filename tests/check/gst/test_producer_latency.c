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
 * A clock-synced interpipesink (sync=true) renders each buffer only once its
 * pipeline's latency has elapsed, so under passthrough-ts and compensate-ts
 * every buffer reaches the consumer that much behind its running time. The
 * consumer must learn about that delay through its own LATENCY query, or every
 * clock-synced element downstream treats the buffers as late.
 *
 * The producer pipeline's latency is pinned with GstPipeline:latency so the
 * value is deterministic rather than whatever audiotestsrc reports. Every
 * consumer sink runs sync=true: GstBaseSink only answers "live" (and GstBin
 * only configures latency) when the sink syncs to the clock.
 */

#define PRODUCER_LATENCY (500 * GST_MSECOND)

#define PRODUCER_DESC \
    "audiotestsrc is-live=true ! interpipesink name=sink sync=true async=false"

#define CONSUMER_DESC(stream_sync) \
    "interpipesrc listen-to=sink is-live=true format=time stream-sync=" \
    stream_sync " ! fakesink name=fsink sync=true async=false"

static GstPipeline *
start_pipeline (const gchar * description)
{
  GstPipeline *pipeline;
  GError *error = NULL;

  pipeline = GST_PIPELINE (gst_parse_launch (description, &error));
  fail_if (error, "%s", error ? error->message : "");

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (pipeline), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (pipeline), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  return pipeline;
}

static void
stop_pipeline (GstPipeline * pipeline)
{
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (pipeline), GST_STATE_NULL));
  gst_object_unref (pipeline);
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

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (producer), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (producer), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  sink = gst_bin_get_by_name (GST_BIN (producer), "sink");
  fail_unless (sink != NULL);
  if (gst_base_sink_get_sync (GST_BASE_SINK (sink)))
    fail_unless_equals_uint64 (gst_base_sink_get_latency (GST_BASE_SINK (sink)),
        latency);
  gst_object_unref (sink);

  return producer;
}

/* The latency the consumer pipeline configured on its sink. */
static GstClockTime
consumer_latency (GstPipeline * consumer)
{
  GstElement *fsink;
  GstClockTime latency;

  fsink = gst_bin_get_by_name (GST_BIN (consumer), "fsink");
  fail_unless (fsink != NULL);
  latency = gst_base_sink_get_latency (GST_BASE_SINK (fsink));
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
    if (GST_CLOCK_TIME_IS_VALID (latency) && latency >= expected)
      break;
    g_usleep (100 * 1000);
  }

  return latency;
}

/* Producer first: the consumer's LATENCY query already sees the producer's
 * latency when the consumer pipeline computes its own on the way to
 * PLAYING. No buffer needs to flow. */
GST_START_TEST (interpipe_producer_latency_reported)
{
  GstPipeline *producer;
  GstPipeline *consumer;
  GstClockTime latency;

  producer = start_producer (PRODUCER_DESC, PRODUCER_LATENCY);
  consumer = start_pipeline (CONSUMER_DESC ("compensate-ts"));

  latency = consumer_latency (consumer);
  fail_unless (GST_CLOCK_TIME_IS_VALID (latency)
      && latency >= PRODUCER_LATENCY,
      "Consumer latency %" GST_TIME_FORMAT " does not include the producer's %"
      GST_TIME_FORMAT, GST_TIME_ARGS (latency),
      GST_TIME_ARGS (PRODUCER_LATENCY));

  stop_pipeline (consumer);
  stop_pipeline (producer);
}

GST_END_TEST;

/* Consumer first: its pipeline computes latency while the node does not exist
 * yet. Once the producer appears and buffers flow, interpipesrc must notice
 * the new producer latency and have the consumer pipeline recompute. */
GST_START_TEST (interpipe_producer_latency_recalculated)
{
  GstPipeline *producer;
  GstPipeline *consumer;
  GstClockTime latency;

  consumer = start_pipeline (CONSUMER_DESC ("compensate-ts"));
  fail_unless_equals_uint64 (consumer_latency (consumer), 0);

  producer = start_producer (PRODUCER_DESC, PRODUCER_LATENCY);

  latency = wait_consumer_latency (consumer, PRODUCER_LATENCY);
  fail_unless (GST_CLOCK_TIME_IS_VALID (latency)
      && latency >= PRODUCER_LATENCY,
      "Consumer latency stayed at %" GST_TIME_FORMAT
      " after the producer came up with %" GST_TIME_FORMAT,
      GST_TIME_ARGS (latency), GST_TIME_ARGS (PRODUCER_LATENCY));

  stop_pipeline (consumer);
  stop_pipeline (producer);
}

GST_END_TEST;

/* restart-ts re-stamps buffers on arrival, so the producer's delay is not
 * visible to the consumer and must not be added. */
GST_START_TEST (interpipe_producer_latency_restart_ts_ignored)
{
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer (PRODUCER_DESC, PRODUCER_LATENCY);
  consumer = start_pipeline (CONSUMER_DESC ("restart-ts"));

  fail_unless_equals_uint64 (consumer_latency (consumer), 0);

  stop_pipeline (consumer);
  stop_pipeline (producer);
}

GST_END_TEST;

/* A producer sink that does not sync to the clock hands buffers over as soon
 * as they arrive, so there is no render latency to report. */
GST_START_TEST (interpipe_producer_latency_unsynced_ignored)
{
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = start_producer ("audiotestsrc is-live=true ! "
      "interpipesink name=sink sync=false async=false", PRODUCER_LATENCY);
  consumer = start_pipeline (CONSUMER_DESC ("compensate-ts"));

  fail_unless_equals_uint64 (consumer_latency (consumer), 0);

  stop_pipeline (consumer);
  stop_pipeline (producer);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("producer_latency");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_producer_latency_reported);
  tcase_add_test (tc, interpipe_producer_latency_recalculated);
  tcase_add_test (tc, interpipe_producer_latency_restart_ts_ignored);
  tcase_add_test (tc, interpipe_producer_latency_unsynced_ignored);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
