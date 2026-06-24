/* GStreamer
 * Copyright (C) 2026 RidgeRun <support@ridgerun.com>
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
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

static gboolean force_key_unit_arrived;

/* Build an upstream force-key-unit event the same way
 * gst_video_event_new_upstream_force_key_unit does, so the test does not need
 * to link gstreamer-video. */
static GstEvent *
new_force_key_unit_event (void)
{
  return gst_event_new_custom (GST_EVENT_CUSTOM_UPSTREAM,
      gst_structure_new ("GstForceKeyUnit",
          "all-headers", G_TYPE_BOOLEAN, TRUE, NULL));
}

static gboolean
get_event_expect_force_key_unit (GstPad * pad, GstObject * parent,
    GstEvent * event)
{
  const GstStructure *structure = gst_event_get_structure (event);

  if (GST_EVENT_TYPE (event) == GST_EVENT_CUSTOM_UPSTREAM && structure
      && gst_structure_has_name (structure, "GstForceKeyUnit")) {
    force_key_unit_arrived = TRUE;
    GST_INFO ("Force-key-unit event arrived to the interpipesink producer");
  }

  return gst_pad_event_default (pad, parent, event);
}

/*
 * Given two listeners sharing one interpipesink, when a force-key-unit
 * upstream event arrives from one interpipesrc it is still forwarded to the
 * interpipesink producer (unlike generic upstream events, which are refused
 * with more than one listener).
 */
GST_START_TEST (interpipe_force_key_unit_two_listeners)
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

  force_key_unit_arrived = FALSE;

  /* Create one sink and two source pipelines */
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
  fail_if (!sinkpad);

  /* Watch the producer's upstream pad for the forwarded event */
  gst_pad_set_event_function (srcpad, get_event_expect_force_key_unit);

  /* Send the force-key-unit event upstream from one consumer. The boolean
   * result of the push is not a reliable signal here: GstBaseSrc returns FALSE
   * for custom upstream events it does not recognise, so what matters is that
   * the event reaches the shared producer, which is asserted below. */
  gst_pad_push_event (sinkpad, new_force_key_unit_event ());

  /* With more than one listener, a generic upstream event would be dropped;
   * the force-key-unit must still reach the producer. */
  fail_unless (force_key_unit_arrived,
      "Force-key-unit was not forwarded to the producer with two listeners");

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesrc,
          GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink,
          GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (pipelinesink2,
          GST_STATE_NULL));

  gst_object_unref (srcpad);
  gst_object_unref (sinkpad);

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
  TCase *tc = tcase_create ("force_key_unit_two_listeners");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_force_key_unit_two_listeners);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
