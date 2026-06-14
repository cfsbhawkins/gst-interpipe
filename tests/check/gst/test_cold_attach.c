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
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/*
 * Cold attach: a live, compensate-ts consumer (interpipesrc is-live=true
 * stream-sync=compensate-ts format=time) is created and set to PLAYING BEFORE
 * the producer node exists or has produced any caps. The producer comes up
 * afterwards. The consumer must still negotiate and deliver buffers, instead of
 * erroring out with "not-negotiated".
 *
 * This is the configuration the existing suite never exercised: test_set_caps
 * uses the default passthrough-ts with no is-live, which negotiates eagerly.
 * The compensate-ts + is-live path is the one tied to the cold-attach
 * negotiation race seen in the multi-pipeline deployment.
 *
 * Scope note: with a flexible converter downstream this scenario already passes
 * on current GStreamer, so this serves as a regression smoke test for the
 * compensate-ts + is-live cold-ordering path. The race only surfaces with a
 * strict downstream encoder that does not renegotiate on late caps; that
 * end-to-end reproduction lives in tests/gstd/ (driven over gstd, where the
 * deployment encoder can be substituted via the ENC override).
 */

/* Fail the test if the pipeline posts an error message on its bus. Returns the
 * error text in *msg (transfer full) when one is found, NULL otherwise. */
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
    if (msg)
      *msg = g_strdup (error->message);
    g_error_free (error);
    gst_message_unref (message);
    has_error = TRUE;
  }
  gst_object_unref (bus);

  return has_error;
}

GST_START_TEST (interpipe_cold_attach_compensate_ts)
{
  GstPipeline *sink;
  GstPipeline *src;
  GstElement *vtest;
  GstElement *asink;
  GstSample *outsample;
  GstCaps *caps;
  gchar *errmsg = NULL;
  GError *error = NULL;

  /*
   * Create and PLAY the consumer first, while the node "sink" does not exist
   * yet. The interpipesrc registers as a listener and waits for the node to
   * appear (cold attach).
   *
   * The downstream videoconvert forces real caps negotiation: it cannot accept
   * a buffer until caps are configured. This is what turns the race into a hard
   * not-negotiated error instead of being silently absorbed by a permissive
   * appsink.
   */
  src =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc listen-to=sink is-live=true format=time "
          "stream-sync=compensate-ts ! videoconvert ! "
          "appsink name=asink async=false", &error));
  fail_if (error);

  asink = gst_bin_get_by_name (GST_BIN (src), "asink");

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src), GST_STATE_PLAYING));

  /*
   * Wait until the consumer is actually PLAYING and let its basesrc loop spin
   * with no producer present, so it runs negotiation while there are still no
   * caps. This is the crux of the cold-attach race: caps then arrive *after*
   * the loop has already negotiated an empty/stale state.
   */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  g_usleep (300000);            /* 300 ms */

  /* Now bring up the producer node, after the consumer is already running. */
  sink =
      GST_PIPELINE (gst_parse_launch
      ("videotestsrc name=vtest is-live=true ! "
          "capsfilter caps=video/x-raw,format=(string)I420,width=320,height=240,framerate=(fraction)30/1 ! "
          "interpipesink name=sink async=false", &error));
  fail_if (error);

  vtest = gst_bin_get_by_name (GST_BIN (sink), "vtest");

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));

  /*
   * A buffer must flow end-to-end through the cold-attached consumer. If the
   * negotiation race regressed, no buffer is ever delivered and this blocks
   * until the check framework times out (deterministic failure).
   */
  outsample = gst_app_sink_pull_sample (GST_APP_SINK (asink));
  fail_if (!outsample, "No buffer delivered to the cold-attached consumer");

  caps = gst_sample_get_caps (outsample);
  fail_if (!caps, "Buffer delivered without caps");

  /* And no not-negotiated (or any) error must have reached either bus. */
  fail_if (pipeline_has_error (src, &errmsg),
      "Consumer pipeline posted an error: %s", errmsg);
  fail_if (pipeline_has_error (sink, &errmsg),
      "Producer pipeline posted an error: %s", errmsg);

  gst_sample_unref (outsample);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src), GST_STATE_NULL));

  /* Cleanup */
  g_free (errmsg);
  g_object_unref (vtest);
  g_object_unref (asink);
  g_object_unref (sink);
  g_object_unref (src);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("cold_attach");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_cold_attach_compensate_ts);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
