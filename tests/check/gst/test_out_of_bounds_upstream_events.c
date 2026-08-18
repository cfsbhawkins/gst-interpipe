/* GStreamer
 * Copyright (C) 2016 Carlos Rodriguez <carlos.rodriguez@ridgerun.com>
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
#include <gst/video/video-event.h>
#include <gst/video/gstvideometa.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/* Counters observed at the PRODUCER side (appsrc src pad). The allow-list
 * contract under test: only the force-key-unit custom event crosses the
 * interpipe boundary; every other upstream event (LATENCY, RECONFIGURE,
 * QOS, SEEK, ...) is dropped at the node regardless of listener count.
 * RECONFIGURE and LATENCY are counted rather than failed outright because
 * the producer legitimately generates its own: add_listener pushes a
 * reconfigure at attach time, and the producer pipeline distributes its own
 * LATENCY event to its sinks, which travels up this same pad. The assertion
 * is that no ADDITIONAL event of either type arrives after the consumer
 * pushes one. */
static gint producer_latency_events;
static gint producer_reconfigure_events;
static gint producer_fku_events;

static gboolean
producer_event_probe (GstPad * pad, GstObject * parent, GstEvent * event)
{
  const GstStructure *s;

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_LATENCY:
      g_atomic_int_inc (&producer_latency_events);
      gst_event_unref (event);
      return TRUE;
    case GST_EVENT_RECONFIGURE:
      g_atomic_int_inc (&producer_reconfigure_events);
      gst_event_unref (event);
      return TRUE;
    case GST_EVENT_CUSTOM_UPSTREAM:
      s = gst_event_get_structure (event);
      if (s && gst_structure_has_name (s, "GstForceKeyUnit"))
        g_atomic_int_inc (&producer_fku_events);
      gst_event_unref (event);
      return TRUE;
    default:
      return gst_pad_event_default (pad, parent, event);
  }
}

/*
 * Given two independent pipelines bridged by interpipe, upstream events
 * pushed from the consumer must NOT reach the producer - except the
 * force-key-unit custom event, which must.
 */
GST_START_TEST (interpipe_out_of_bounds_upstream_events_one_listener)
{
  GstElement *pipelinesrc;
  GstElement *pipelinesink;
  GstElement *appsrc;
  GstElement *intersink;
  GstElement *intersrc;
  GstElement *fsink;
  GstPad *srcpad;
  GstPad *sinkpad;
  GError *error = NULL;
  gint reconfigures_after_attach;
  gint latencies_after_attach;

  producer_latency_events = 0;
  producer_reconfigure_events = 0;
  producer_fku_events = 0;

  /* Create one sink and one source pipelines */
  pipelinesrc = gst_pipeline_new ("src_pipe");
  pipelinesink = gst_pipeline_new ("sink_pipe");

  intersink =
      gst_parse_launch ("interpipesink name=videosrc1 sync=true", &error);
  fail_if (error);

  appsrc = gst_parse_launch ("appsrc name=appsrc", &error);
  fail_if (error);

  intersrc =
      gst_parse_launch ("interpipesrc name=display listen-to=videosrc1",
      &error);
  fail_if (error);

  fsink = gst_parse_launch ("fakesink sync=true async=false", &error);
  fail_if (error);

  gst_bin_add_many (GST_BIN (pipelinesrc), appsrc, intersink, NULL);
  gst_element_link_many (appsrc, intersink, NULL);
  gst_bin_add_many (GST_BIN (pipelinesink), intersrc, fsink, NULL);
  gst_element_link_many (intersrc, fsink, NULL);

  /* Play the pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesrc,
          GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink,
          GST_STATE_PLAYING));

  /* Create pads */
  srcpad = gst_element_get_static_pad (appsrc, "src");
  fail_if (!srcpad);
  sinkpad = gst_element_get_static_pad (fsink, "sink");
  fail_if (!sinkpad);

  /* Observe events at the producer */
  gst_pad_set_event_function (srcpad, producer_event_probe);

  /* Let the attach-time reconfigure (a producer-side event from
   * add_listener) land, then snapshot. */
  g_usleep (200000);
  reconfigures_after_attach = g_atomic_int_get (&producer_reconfigure_events);
  latencies_after_attach = g_atomic_int_get (&producer_latency_events);

  /* Consumer-originated upstream events: LATENCY and RECONFIGURE must be
   * dropped at the node; force-key-unit must be forwarded. The FKU push
   * return is deliberately ignored: interpipesrc relays the event to the
   * node and then chains to GstBaseSrc's default handler, which returns
   * FALSE for custom events it does not recognize — delivery is asserted
   * on the producer-side counter below instead. */
  fail_if (!gst_pad_push_event (sinkpad, gst_event_new_latency (1)));
  fail_if (!gst_pad_push_event (sinkpad, gst_event_new_reconfigure ()));
  (void) gst_pad_push_event (sinkpad,
      gst_video_event_new_upstream_force_key_unit (GST_CLOCK_TIME_NONE,
          TRUE, 1));

  g_usleep (200000);

  fail_unless_equals_int (g_atomic_int_get (&producer_latency_events),
      latencies_after_attach);
  fail_unless_equals_int (g_atomic_int_get (&producer_reconfigure_events),
      reconfigures_after_attach);
  fail_unless (g_atomic_int_get (&producer_fku_events) >= 1);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesrc,
          GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink,
          GST_STATE_NULL));

  /* Cleanup */
  gst_object_unref (srcpad);
  gst_object_unref (sinkpad);
  g_object_unref (pipelinesrc);
  g_object_unref (pipelinesink);
}

GST_END_TEST;

static gboolean
get_event_two_listeners (GstPad * pad, GstObject * parent, GstEvent * event)
{
  gboolean ret = FALSE;
  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_LATENCY:
      ret = TRUE;
      GST_ERROR
          ("Latency test event arrives to the interpipesink and it should not");
      break;
    default:
      ret = gst_pad_event_default (pad, parent, event);
      break;
  }
  fail_if (ret);
  return TRUE;
}
/*
 * Given three independent pipelines, when an
 * upstream event arrives to one interpipesrc
 * it will try to be forwarded to the interpipesink,
 * but it can be send because there is another listener. 
 */
GST_START_TEST (interpipe_out_of_bounds_upstream_events_two_listeners)
{
  GstElement *pipelinesrc;
  GstElement *pipelinesink;
  GstElement *pipelinesink2;
  GstElement *appsrc;
  GstElement *intersink;
  GstElement *intersrc;
  GstElement *fsink;
  GstElement *intersrc2;
  GstElement *fsink2;
  GstPad *srcpad;
  GstPad *sinkpad;
  GError *error = NULL;
  GstClockTime latency;

  /* Create one sink and one source pipelines */
  pipelinesrc = gst_pipeline_new ("src_pipe");
  pipelinesink = gst_pipeline_new ("sink_pipe");
  pipelinesink2 = gst_pipeline_new ("sink_pipe2");

  intersink =
      gst_parse_launch ("interpipesink name=videosrc1 sync=true", &error);
  fail_if (error);

  appsrc = gst_parse_launch ("appsrc name=appsrc", &error);
  fail_if (error);

  intersrc =
      gst_parse_launch ("interpipesrc name=display listen-to=videosrc1",
      &error);
  fail_if (error);

  fsink = gst_parse_launch ("fakesink sync=true async=false", &error);
  fail_if (error);

  intersrc2 =
      gst_parse_launch ("interpipesrc name=display2 listen-to=videosrc1",
      &error);
  fail_if (error);

  fsink2 = gst_parse_launch ("fakesink sync=true async=false", &error);
  fail_if (error);


  gst_bin_add_many (GST_BIN (pipelinesrc), appsrc, intersink, NULL);
  gst_element_link_many (appsrc, intersink, NULL);
  gst_bin_add_many (GST_BIN (pipelinesink), intersrc, fsink, NULL);
  gst_element_link_many (intersrc, fsink, NULL);
  gst_bin_add_many (GST_BIN (pipelinesink2), intersrc2, fsink2, NULL);
  gst_element_link_many (intersrc2, fsink2, NULL);

  /* Play the pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesrc,
          GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink,
          GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink2,
          GST_STATE_PLAYING));

  /* Create pads */
  srcpad = gst_element_get_static_pad (appsrc, "src");
  fail_if (!srcpad);
  sinkpad = gst_element_get_static_pad (fsink, "sink");
  fail_if (!srcpad);

  /* Set event function on pad */
  gst_pad_set_event_function (srcpad, get_event_two_listeners);

  /* Send Latency event */
  latency = 1;
  fail_if (!gst_pad_push_event (sinkpad, gst_event_new_latency (latency)));

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesrc,
          GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink,
          GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink2,
          GST_STATE_NULL));

  /* Cleanup */
  g_object_unref (pipelinesrc);
  g_object_unref (pipelinesink);
  g_object_unref (pipelinesink2);
}

GST_END_TEST;



static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("out_of_bounds_upstream_events_one_listener");
  TCase *tc2 = tcase_create ("out_of_bounds_upstream_events_two_listeners");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_out_of_bounds_upstream_events_one_listener);

  suite_add_tcase (suite, tc2);
  tcase_add_test (tc2, interpipe_out_of_bounds_upstream_events_two_listeners);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
