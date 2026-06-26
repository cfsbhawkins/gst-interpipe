/* GStreamer
 * Copyright (C) 2026 RidgeRun, LLC (http://www.ridgerun.com)
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

/*
 * When the first element downstream of an interpipesrc advertises every raw
 * format (e.g. videocrop, whose crop is set at runtime so it accepts any
 * format/size), the consumer leg must still negotiate the format the producer
 * is actually delivering, not the first format that downstream happens to
 * list. Otherwise the producer delivers one format while the leg is configured
 * for another and negotiation fails.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/check/gstcheck.h>
#include <gst/app/gstappsink.h>

GST_START_TEST (interpipe_videocrop_producer_format_wins)
{
  GstPipeline *sink;
  GstPipeline *src;
  GstElement *asink;
  GstSample *sample;
  GstCaps *caps;
  GstStructure *st;
  const gchar *format;
  GError *error = NULL;

  /* Producer: videotestsrc through videoconvert into the node, with no format
   * pinned. videotestsrc delivers I420 by default. */
  sink = GST_PIPELINE (gst_parse_launch
      ("videotestsrc is-live=true ! "
          "video/x-raw,width=320,height=240,framerate=(fraction)30/1 ! "
          "videoconvert ! interpipesink name=vcnode forward-events=true "
          "forward-eos=false sync=false async=false", &error));
  fail_if (error);

  /* Consumer: videocrop is the first element after interpipesrc. videocrop
   * advertises every format/size, so the leg must adopt the producer's I420
   * rather than videocrop's first-listed format. */
  src = GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=isrc listen-to=vcnode is-live=true format=time "
          "stream-sync=compensate-ts ! queue ! "
          "videocrop top=0 left=0 right=0 bottom=0 ! videoconvert ! "
          "appsink name=asink async=false sync=false", &error));
  fail_if (error);

  asink = gst_bin_get_by_name (GST_BIN (src), "asink");

  /* Producer first, so the consumer attaches to an already-running producer. */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (sink), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src), GST_STATE_PLAYING));

  /* A buffer arrives only if the leg negotiated. NULL means the leg never
   * negotiated. Bounded so the test cannot hang. */
  sample = gst_app_sink_try_pull_sample (GST_APP_SINK (asink), 5 * GST_SECOND);
  fail_unless (sample != NULL,
      "no buffer received: consumer leg failed to negotiate");

  caps = gst_sample_get_caps (sample);
  fail_unless (caps != NULL);
  st = gst_caps_get_structure (caps, 0);
  format = gst_structure_get_string (st, "format");
  fail_unless_equals_string (format, "I420");

  gst_sample_unref (sample);

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));

  g_object_unref (asink);
  g_object_unref (sink);
  g_object_unref (src);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("videocrop_format");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_videocrop_producer_format_wins);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
