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

#include <unistd.h>
#include <gst/check/gstcheck.h>
#include <gst/video/gstvideometa.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/* Caps reach a listener through the producer's renegotiation, on the
 * producer's streaming thread. A sample delivered to the listener's appsink
 * proves that has happened, and that the listener took the caps. */
static void
wait_for_sample (GstPipeline * pipeline, const gchar * appsink_name)
{
  GstElement *appsink;
  GstSample *sample;

  appsink = gst_bin_get_by_name (GST_BIN (pipeline), appsink_name);
  fail_unless (appsink != NULL);
  sample = gst_app_sink_pull_sample (GST_APP_SINK (appsink));
  fail_if (!sample, "No buffer delivered to %s", appsink_name);
  gst_sample_unref (sample);
  gst_object_unref (appsink);
}

/* Returns TRUE if the pipeline posted an error message on its bus, with the
 * error text in *msg (transfer full). */
static gboolean
pipeline_has_error (GstPipeline * pipeline, gchar ** msg)
{
  GstBus *bus;
  GstMessage *message;
  gboolean has_error = FALSE;

  bus = gst_pipeline_get_bus (pipeline);
  message = gst_bus_poll (bus, GST_MESSAGE_ERROR, 0);
  if (message) {
    GError *error = NULL;

    gst_message_parse_error (message, &error, NULL);
    *msg = g_strdup (error->message);
    g_error_free (error);
    gst_message_unref (message);
    has_error = TRUE;
  }
  gst_object_unref (bus);

  return has_error;
}

/*
 * Given a interpipesrc with fixed caps, after the get caps,
 * the only valid caps on the interpipesrc are the same 
 * fixed caps.
 */

GST_START_TEST (interpipe_get_caps_one_interpipesrc)
{
  GstPipeline *sink;
  GstPipeline *src;
  GstElement *intersink;
  GstElement *intersrc;
  GstCaps *caps1;
  GstCaps *caps2;
  GError *error = NULL;
  gchar *errmsg = NULL;

  /* Create one sink and two source pipelines */
  sink =
      GST_PIPELINE (gst_parse_launch ("videotestsrc name=vtest ! interpipesink "
          "name=intersink async=true", &error));
  fail_if (error);

  intersink = gst_bin_get_by_name (GST_BIN (sink), "intersink");

  src =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=[320,640],height=[240,480],framerate=(fraction)30/1 ! "
          "appsink name=asink async=false", &error));
  fail_if (error);

  intersrc = gst_bin_get_by_name (GST_BIN (src), "intersrc");

  /* Ready the pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (sink), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (GST_ELEMENT (src),
          GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_get_state (GST_ELEMENT (src),
          NULL, NULL, GST_CLOCK_TIME_NONE));

  wait_for_sample (src, "asink");
  fail_if (pipeline_has_error (sink, &errmsg),
      "Producer pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (src, &errmsg),
      "Listener pipeline posted an error: %s", errmsg);

  /* Verifies if interpipesink and interpipesrcs have the same caps
   */
  caps1 = gst_app_src_get_caps (GST_APP_SRC (intersrc));
  fail_if (!caps1);

  caps2 = gst_app_sink_get_caps (GST_APP_SINK (intersink));
  fail_if (!caps2);

  fail_if (!gst_caps_is_equal (caps1, caps2));

  gst_caps_unref (caps1);
  gst_caps_unref (caps2);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (GST_ELEMENT (src),
          GST_STATE_NULL));

  /* Cleanup */
  g_free (errmsg);
  g_object_unref (intersink);
  g_object_unref (intersrc);
  g_object_unref (sink);
  g_object_unref (src);
}

GST_END_TEST;

/* 
 * Given two interpipesrc, the get caps function will
 * configure the intersection with the sink caps.
 */

GST_START_TEST (interpipe_get_caps_two_interpipesrcs_intersection)
{
  GstPipeline *sink;
  GstPipeline *src1;
  GstPipeline *src2;
  GstElement *intersink;
  GstElement *intersrc1;
  GstElement *intersrc2;
  GstCaps *caps1;
  GstCaps *caps2;
  GstCaps *caps3;
  GError *error = NULL;
  gchar *errmsg = NULL;

  /* Create one sink and two source pipelines */
  sink =
      GST_PIPELINE (gst_parse_launch ("videotestsrc name=vtest ! interpipesink "
          "name=intersink async=true", &error));
  fail_if (error);

  intersink = gst_bin_get_by_name (GST_BIN (sink), "intersink");

  src1 =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc1 listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=[320,640],height=[240,480],framerate=(fraction)30/1 ! "
          "appsink name=asink1 async=false", &error));
  fail_if (error);

  intersrc1 = gst_bin_get_by_name (GST_BIN (src1), "intersrc1");

  src2 =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc2 listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=[500,640],height=[300,480],framerate=(fraction)30/1 ! "
          "appsink name=asink2 async=false", &error));
  fail_if (error);

  intersrc2 = gst_bin_get_by_name (GST_BIN (src2), "intersrc2");

  /* Ready the pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (sink), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src1), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src1), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src2), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src2), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  /* The second listener cannot take the caps negotiated for the first, so
   * the node renegotiates upstream to the intersection of both. Each
   * listener's caps are final once a buffer has reached it. */
  wait_for_sample (src1, "asink1");
  wait_for_sample (src2, "asink2");
  fail_if (pipeline_has_error (sink, &errmsg),
      "Producer pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (src1, &errmsg),
      "First listener pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (src2, &errmsg),
      "Second listener pipeline posted an error: %s", errmsg);

  /* Verifies if interpipesink and interpipesrcs have the same caps
   */

  caps1 = gst_app_src_get_caps (GST_APP_SRC (intersrc1));
  fail_if (gst_caps_is_empty (caps1));

  caps2 = gst_app_src_get_caps (GST_APP_SRC (intersrc2));
  fail_if (gst_caps_is_empty (caps2));

  caps3 = gst_app_sink_get_caps (GST_APP_SINK (intersink));
  fail_if (gst_caps_is_empty (caps3));

  fail_if (!gst_caps_is_equal (caps1, caps2));
  fail_if (!gst_caps_is_equal (caps2, caps3));

  gst_caps_unref (caps1);
  gst_caps_unref (caps2);
  gst_caps_unref (caps3);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src1), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src2), GST_STATE_NULL));

  /* Cleanup */
  g_free (errmsg);
  g_object_unref (intersink);
  g_object_unref (intersrc1);
  g_object_unref (intersrc2);
  g_object_unref (sink);
  g_object_unref (src1);
  g_object_unref (src2);
}

GST_END_TEST;

/*
 * Given two interpipesrc with no intersection, the node cannot serve both
 * with one stream. It must log the failed intersection and detach every
 * listener rather than feed either of them caps it cannot accept, and no
 * pipeline may error out.
 */

GST_START_TEST (interpipe_get_caps_two_interpipesrcs_no_intersection)
{
  GstPipeline *sink;
  GstPipeline *src1;
  GstPipeline *src2;
  GstElement *intersink;
  GstElement *intersrc1;
  GstElement *intersrc2;
  guint num_listeners = 1;
  gint i;
  GError *error = NULL;
  gchar *errmsg = NULL;

  /* Create one sink and two source pipelines */
  sink =
      GST_PIPELINE (gst_parse_launch ("videotestsrc name=vtest ! interpipesink "
          "name=intersink async=true", &error));
  fail_if (error);

  intersink = gst_bin_get_by_name (GST_BIN (sink), "intersink");

  src1 =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc1 listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=[720,1280],height=[640,1080],framerate=(fraction)30/1 ! "
          "appsink name=asink1 async=false", &error));
  fail_if (error);

  intersrc1 = gst_bin_get_by_name (GST_BIN (src1), "intersrc1");

  src2 =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc2 listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=[500,640],height=[300,480],framerate=(fraction)30/1 ! "
          "appsink name=asink2 async=false", &error));
  fail_if (error);

  intersrc2 = gst_bin_get_by_name (GST_BIN (src2), "intersrc2");

  /* Ready the pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (sink), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src1), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src1), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src2), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src2), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  /* The renegotiation runs on the producer's streaming thread; give it time
   * to fail the intersection and detach both listeners. */
  for (i = 0; i < 50; i++) {
    g_object_get (intersink, "num-listeners", &num_listeners, NULL);
    if (num_listeners == 0)
      break;
    g_usleep (100 * 1000);
  }
  fail_unless_equals_int (num_listeners, 0);

  fail_if (pipeline_has_error (sink, &errmsg),
      "Producer pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (src1, &errmsg),
      "First listener pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (src2, &errmsg),
      "Second listener pipeline posted an error: %s", errmsg);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src1), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src2), GST_STATE_NULL));

  /* Cleanup */
  g_free (errmsg);
  g_object_unref (intersink);
  g_object_unref (intersrc1);
  g_object_unref (intersrc2);
  g_object_unref (sink);
  g_object_unref (src1);
  g_object_unref (src2);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc1 = tcase_create ("get_caps_one_interpipesrc");
  TCase *tc2 = tcase_create ("get_caps_two_interpipesrcs_intersection");
  TCase *tc3 = tcase_create ("get_caps_two_interpipesrcs_no_intersection");

  suite_add_tcase (suite, tc1);
  tcase_add_test (tc1, interpipe_get_caps_one_interpipesrc);

  suite_add_tcase (suite, tc2);
  tcase_add_test (tc2, interpipe_get_caps_two_interpipesrcs_intersection);

  suite_add_tcase (suite, tc3);
  tcase_add_test (tc3, interpipe_get_caps_two_interpipesrcs_no_intersection);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
