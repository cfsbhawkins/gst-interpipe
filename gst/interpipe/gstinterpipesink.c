/* GStreamer
 * Copyright (C) 2013-2016 Michael Grüner <michael.gruner@ridgerun.com>
 * Copyright (C) 2014 Jose Jimenez <jose.jimenez@ridgerun.com>
 * Copyright (C) 2016 Carlos Rodriguez <carlos.rodriguez@ridgerun.com>
 * Copyright (C) 2016 Erick Arroyo <erick.arroyo@ridgerun.com>
 * Copyright (C) 2016 Marco Madrigal <marco.madrigal@ridgerun.com>
 *
 * This file is part of gst-interpipe-1.0
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
/**
 * SECTION:gstinterpipesink
 * @see_also: #GstInterPipeSrc
 *
 * Sink element for interpipeline communication
 *
 * <refsect2>
 * <title>Example launch line</title>
 * |[
 * gst-launch \
 *   videotestsrc ! interpipesink name=test \
 *   interpipesrc listen-to=test ! xvimagesink
 * ]| Send buffers across two different pipelines
 * </refsect2>
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <gst/gst.h>

#include "gstinterpipesink.h"
#include "gstinterpipeinode.h"

GST_DEBUG_CATEGORY_STATIC (gst_inter_pipe_sink_debug);
#define GST_CAT_DEFAULT gst_inter_pipe_sink_debug

enum
{
  PROP_0,
  /* Offset own property ids well above the inherited GstAppSink/GstBaseSink
   * range so they never collide with parent property ids (sync, async, ...).
   * Inherited properties fall through to the chained-up default case. */
  PROP_FORWARD_EOS = 0x100,
  PROP_FORWARD_EVENTS,
  PROP_NUM_LISTENERS
};

static void gst_inter_pipe_sink_update_node_name (GstInterPipeSink * sink,
    GParamSpec * pspec);
static void gst_inter_pipe_sink_set_property (GObject * object, guint prop_id,
    const GValue * value, GParamSpec * pspec);
static void gst_inter_pipe_sink_get_property (GObject * object, guint prop_id,
    GValue * value, GParamSpec * pspec);
static void gst_inter_pipe_sink_finalize (GObject * object);
static GstFlowReturn gst_inter_pipe_sink_new_buffer (GstAppSink * sink,
    gpointer data);
static GstFlowReturn gst_inter_pipe_sink_new_preroll (GstAppSink * asink,
    gpointer data);
static void gst_inter_pipe_sink_eos (GstAppSink * sink, gpointer data);
static gboolean gst_inter_pipe_sink_add_listener (GstInterPipeINode * iface,
    GstInterPipeIListener * listener);
static gboolean gst_inter_pipe_sink_remove_listener (GstInterPipeINode * iface,
    GstInterPipeIListener * listener);
static gboolean gst_inter_pipe_sink_receive_event (GstInterPipeINode * iface,
    GstEvent * event);
static GstCaps *gst_inter_pipe_sink_get_caps (GstBaseSink * base,
    GstCaps * filter);
static gboolean gst_inter_pipe_sink_set_caps (GstBaseSink * base,
    GstCaps * filter);
static gboolean gst_inter_pipe_sink_event (GstBaseSink * base,
    GstEvent * event);
static gboolean gst_inter_pipe_sink_stop (GstBaseSink * base);
static gboolean gst_inter_pipe_sink_send_event (GstElement * element,
    GstEvent * event);
static void gst_inter_pipe_sink_delay_changed (GObject * object,
    GParamSpec * pspec, gpointer user_data);
static gboolean gst_inter_pipe_sink_propose_allocation (GstBaseSink * base,
    GstQuery * query);
static gboolean gst_inter_pipe_sink_are_caps_compatible (GstInterPipeSink *
    sink, GstCaps * listener_caps, GstCaps * sinkcaps);
static GstCaps *gst_inter_pipe_sink_caps_intersect (GstCaps * caps1,
    GstCaps * caps2);
static void gst_inter_pipe_sink_forward_event (gpointer key, gpointer value,
    gpointer user_data);

static void gst_inter_pipe_inode_init (GstInterPipeINodeInterface * iface);

#define GST_INTER_PIPE_SINK_PAD(obj)                 (GST_BASE_SINK_CAST (obj)->sinkpad)

struct _GstInterPipeSink
{
  GstAppSink parent;

  /** Node name */
  gchar *node_name;

  /** The list of listeners */
  GHashTable *listeners;

  /** Enable Events notify */
  gboolean forward_events;

  /** Enable EOS notify */
  gboolean forward_eos;

  /** Last caps */
  GstCaps *caps;

  /** Negotiated caps  **/
  GstCaps *caps_negotiated;

  /** Last buffer timestamp */
  guint64 last_buffer_timestamp;

  /* The buffer forwarded from preroll because the pipeline was staying
   * paused, kept until the base sink renders it so that render can recognise
   * it and not forward it a second time. Holding the ref keeps the pointer
   * from being reused for another buffer in the meantime. Guarded by the
   * object lock. */
  GstBuffer *preroll_buffer;

  /* Listeners that set_caps could not give the newly negotiated caps (they
   * attached after get_caps computed them). Their buffers are dropped until
   * the renegotiation set_caps requested runs. Borrowed pointers, removed in
   * remove_listener. Guarded by listeners_mutex. */
  GHashTable *awaiting_caps;

  GMutex listeners_mutex;
};

struct _GstInterPipeSinkClass
{
  GstAppSinkClass parent_class;
};

G_DEFINE_TYPE_WITH_CODE (GstInterPipeSink, gst_inter_pipe_sink,
    GST_TYPE_APP_SINK, G_IMPLEMENT_INTERFACE (GST_INTER_PIPE_TYPE_INODE,
        gst_inter_pipe_inode_init));

static void
gst_inter_pipe_sink_class_init (GstInterPipeSinkClass * klass)
{
  GObjectClass *gobject_class;
  GstElementClass *element_class;
  GstBaseSinkClass *basesink_class;

  gobject_class = G_OBJECT_CLASS (klass);
  element_class = GST_ELEMENT_CLASS (klass);
  basesink_class = GST_BASE_SINK_CLASS (klass);

  GST_DEBUG_CATEGORY_INIT (gst_inter_pipe_sink_debug, "interpipesink", 0,
      "interpipeline sink");

  gst_element_class_set_static_metadata (element_class,
      "Internal pipeline sink",
      "Generic/Sink",
      "Sink for internal pipeline buffers communication",
      "Michael Grüner <michael.gruner@ridgerun.com>");

  gobject_class->set_property = gst_inter_pipe_sink_set_property;
  gobject_class->get_property = gst_inter_pipe_sink_get_property;
  gobject_class->finalize = gst_inter_pipe_sink_finalize;

  g_object_class_install_property (gobject_class, PROP_FORWARD_EOS,
      g_param_spec_boolean ("forward-eos", "Forward EOS",
          "Forward the EOS event to all the listeners",
          FALSE, G_PARAM_WRITABLE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_FORWARD_EVENTS,
      g_param_spec_boolean ("forward-events", "Forward events",
          "Forward downstream events to all the listeners (except for EOS)",
          TRUE, G_PARAM_WRITABLE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_NUM_LISTENERS,
      g_param_spec_uint ("num-listeners", "Number of listeners",
          "Number of interpipe sources listening to this specific sink",
          0, G_MAXUINT, 0, G_PARAM_READABLE));

  basesink_class->get_caps = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_get_caps);
  basesink_class->set_caps = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_set_caps);
  basesink_class->event = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_event);
  basesink_class->stop = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_stop);
  element_class->send_event =
      GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_send_event);
  basesink_class->propose_allocation =
      GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_propose_allocation);
}

static void
gst_inter_pipe_sink_update_node_name (GstInterPipeSink * sink,
    GParamSpec * pspec)
{
  GstInterPipeINode *node;

  node = GST_INTER_PIPE_INODE (sink);

  if (sink->node_name) {
    gst_inter_pipe_remove_node (node, sink->node_name);
    g_free (sink->node_name);
  }

  sink->node_name = gst_object_get_name (GST_OBJECT (sink));
  gst_inter_pipe_add_node (node, sink->node_name);
}

static void
gst_inter_pipe_sink_init (GstInterPipeSink * sink)
{
  GstAppSinkCallbacks callbacks;

  sink->caps = NULL;
  sink->caps_negotiated = NULL;
  sink->node_name = NULL;
  sink->listeners = g_hash_table_new (g_direct_hash, g_direct_equal);
  sink->forward_eos = FALSE;
  sink->forward_events = TRUE;
  sink->last_buffer_timestamp = 0;
  sink->preroll_buffer = NULL;
  sink->awaiting_caps = g_hash_table_new (g_direct_hash, g_direct_equal);

  /* These change how long after its running time this sink hands a buffer
   * over, which listeners report as latency. */
  g_signal_connect (sink, "notify::sync",
      G_CALLBACK (gst_inter_pipe_sink_delay_changed), NULL);
  g_signal_connect (sink, "notify::ts-offset",
      G_CALLBACK (gst_inter_pipe_sink_delay_changed), NULL);
  g_signal_connect (sink, "notify::render-delay",
      G_CALLBACK (gst_inter_pipe_sink_delay_changed), NULL);

  g_mutex_init (&sink->listeners_mutex);

  /* Set the struct buffer to 0's so if in the future more callbacks are added
   * does not cause a segmentation fault down the line
   */
  memset (&callbacks, 0, sizeof (callbacks));

  /* AppSink callbacks */
  callbacks.eos = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_eos);
  callbacks.new_sample = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_new_buffer);
  callbacks.new_preroll = GST_DEBUG_FUNCPTR (gst_inter_pipe_sink_new_preroll);
  gst_app_sink_set_callbacks (GST_APP_SINK (sink), &callbacks, NULL, NULL);

  /*AppSink configuration */
  gst_app_sink_set_drop (GST_APP_SINK (sink), TRUE);
  gst_base_sink_set_sync (GST_BASE_SINK (sink), FALSE);
  gst_app_sink_set_max_buffers (GST_APP_SINK (sink), 3);

  /* When a change in the interpipesink name happens, the callback function
     will update the node name and the nodes list */
  g_object_notify (G_OBJECT (sink), "name");

  g_signal_connect (sink, "notify::name",
      G_CALLBACK (gst_inter_pipe_sink_update_node_name), NULL);
}


static void
gst_inter_pipe_sink_set_property (GObject * object, guint prop_id,
    const GValue * value, GParamSpec * pspec)
{
  GstInterPipeSink *sink;

  g_return_if_fail (GST_IS_INTER_PIPE_SINK (object));

  sink = GST_INTER_PIPE_SINK (object);

  switch (prop_id) {
    case PROP_FORWARD_EOS:
      sink->forward_eos = g_value_get_boolean (value);
      break;
    case PROP_FORWARD_EVENTS:
      sink->forward_events = g_value_get_boolean (value);
      break;
    default:
      /* Chain inherited GstAppSink/GstBaseSink properties (sync, async, ...)
       * up to the parent so they are actually applied. */
      G_OBJECT_CLASS (gst_inter_pipe_sink_parent_class)->set_property (object,
          prop_id, value, pspec);
      break;
  }
}

static void
gst_inter_pipe_sink_get_property (GObject * object, guint prop_id,
    GValue * value, GParamSpec * pspec)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;

  g_return_if_fail (GST_IS_INTER_PIPE_SINK (object));

  sink = GST_INTER_PIPE_SINK (object);
  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);

  switch (prop_id) {
    case PROP_NUM_LISTENERS:
      g_mutex_lock (&sink->listeners_mutex);
      g_value_set_uint (value, g_hash_table_size (listeners));
      g_mutex_unlock (&sink->listeners_mutex);
      break;
    default:
      G_OBJECT_CLASS (gst_inter_pipe_sink_parent_class)->get_property (object,
          prop_id, value, pspec);
      break;
  }
}

static void
gst_inter_pipe_sink_finalize (GObject * object)
{
  GstInterPipeSink *sink;
  GstInterPipeINode *node;

  sink = GST_INTER_PIPE_SINK (object);
  node = GST_INTER_PIPE_INODE (sink);

  if (sink->node_name != NULL) {
    GST_DEBUG_OBJECT (sink, "Removing node %s and associated listeners",
        sink->node_name);
    gst_inter_pipe_remove_node (node, sink->node_name);
    g_free (sink->node_name);
  }

  if (sink->caps) {
    gst_caps_unref (sink->caps);
  }

  if (sink->caps_negotiated) {
    gst_caps_unref (sink->caps_negotiated);
  }

  gst_buffer_replace (&sink->preroll_buffer, NULL);

  g_hash_table_destroy (sink->listeners);
  g_hash_table_destroy (sink->awaiting_caps);

  g_mutex_clear (&sink->listeners_mutex);

  /* Chain up to the parent class */
  G_OBJECT_CLASS (gst_inter_pipe_sink_parent_class)->finalize (object);
}

static gboolean
gst_inter_pipe_sink_are_caps_compatible (GstInterPipeSink * sink,
    GstCaps * listener_caps, GstCaps * sinkcaps)
{
  GstCaps *renegotiated_caps;
  gboolean compatible = TRUE;

  renegotiated_caps = gst_caps_intersect (listener_caps, sinkcaps);

  if (gst_caps_is_empty (renegotiated_caps)) {
    GST_ERROR_OBJECT (sink, "No caps intersection between listener and sink");
    compatible = FALSE;
  } else {
    GST_INFO_OBJECT (sink, "Renegotiated caps: %" GST_PTR_FORMAT,
        renegotiated_caps);
  }

  gst_caps_unref (renegotiated_caps);
  return compatible;
}

static GstCaps *
gst_inter_pipe_sink_caps_intersect (GstCaps * caps1, GstCaps * caps2)
{
  if (!caps1 && !caps2) {
    return NULL;
  }

  if (!caps1) {
    return gst_caps_ref (caps2);
  }

  if (!caps2) {
    return gst_caps_ref (caps1);
  }

  return gst_caps_intersect (caps1, caps2);
}

/* A listener and the caps its downstream accepts, as queried by
 * gst_inter_pipe_sink_query_listener_caps. */
typedef struct
{
  GstInterPipeIListener *listener;
  GstCaps *caps;
  gboolean primed;
} GstInterPipeSinkListenerCaps;

/* Snapshot the registered listeners (only those still waiting for caps when
 * only_unprimed is set), holding a ref on each, and ask each one which caps its
 * downstream accepts. The caps queries go to other pipelines' pads, which can
 * block or come back into this element, so they run without listeners_mutex:
 * the lock is held only while copying the table. Free the result with
 * gst_inter_pipe_sink_free_listener_caps. */
static GArray *
gst_inter_pipe_sink_query_listener_caps (GstInterPipeSink * sink,
    gboolean only_unprimed)
{
  GArray *entries;
  GHashTableIter iter;
  gpointer value;
  guint i;

  entries = g_array_new (FALSE, TRUE, sizeof (GstInterPipeSinkListenerCaps));

  g_mutex_lock (&sink->listeners_mutex);
  g_hash_table_iter_init (&iter, GST_INTER_PIPE_SINK_LISTENERS (sink));
  while (g_hash_table_iter_next (&iter, NULL, &value)) {
    GstInterPipeSinkListenerCaps entry = { NULL, NULL, FALSE };

    entry.primed = gst_inter_pipe_ilistener_is_negotiated (value);
    if (only_unprimed && entry.primed)
      continue;
    entry.listener = gst_object_ref (value);
    g_array_append_val (entries, entry);
  }
  g_mutex_unlock (&sink->listeners_mutex);

  for (i = 0; i < entries->len; i++) {
    GstInterPipeSinkListenerCaps *entry =
        &g_array_index (entries, GstInterPipeSinkListenerCaps, i);
    gboolean negotiated;

    entry->caps = gst_inter_pipe_ilistener_get_caps (entry->listener,
        &negotiated);
    GST_INFO_OBJECT (sink, "Listener %s caps: %" GST_PTR_FORMAT,
        gst_inter_pipe_ilistener_get_name (entry->listener), entry->caps);
  }

  return entries;
}

static void
gst_inter_pipe_sink_free_listener_caps (GArray * entries)
{
  guint i;

  for (i = 0; i < entries->len; i++) {
    GstInterPipeSinkListenerCaps *entry =
        &g_array_index (entries, GstInterPipeSinkListenerCaps, i);

    gst_object_unref (entry->listener);
    if (entry->caps)
      gst_caps_unref (entry->caps);
  }
  g_array_free (entries, TRUE);
}

static const GstInterPipeSinkListenerCaps *
gst_inter_pipe_sink_find_listener_caps (GArray * entries,
    GstInterPipeIListener * listener)
{
  guint i;

  for (i = 0; entries && i < entries->len; i++) {
    const GstInterPipeSinkListenerCaps *entry =
        &g_array_index (entries, GstInterPipeSinkListenerCaps, i);

    if (entry->listener == listener)
      return entry;
  }
  return NULL;
}

/* Detach each listener in the list from this node and release the list. Must
 * be called without listeners_mutex: leaving re-enters this element through
 * remove_listener. */
static void
gst_inter_pipe_sink_detach_listeners (GstInterPipeSink * sink, GList * evict)
{
  GList *l;

  for (l = evict; l != NULL; l = l->next) {
    GstInterPipeIListener *listener = l->data;

    GST_WARNING_OBJECT (sink, "Detaching listener %s: no caps it accepts can "
        "be negotiated on this node", gst_inter_pipe_ilistener_get_name
        (listener));
    if (!gst_inter_pipe_leave_node (listener))
      GST_WARNING_OBJECT (listener, "Unable to remove listener from node");
    gst_object_unref (listener);
  }
  g_list_free (evict);
}

static GstCaps *
gst_inter_pipe_sink_get_caps (GstBaseSink * base, GstCaps * filter)
{
  GstInterPipeSink *sink;
  GArray *entries;
  GstCaps *negotiated = NULL;
  GstCaps *result = NULL;
  GList *evict = NULL;
  gint pass;
  guint i;

  sink = GST_INTER_PIPE_SINK (base);

  entries = gst_inter_pipe_sink_query_listener_caps (sink, FALSE);
  if (0 == entries->len) {
    GST_INFO_OBJECT (sink, "No listeners yet, accepting any caps");
    gst_inter_pipe_sink_free_listener_caps (entries);
    return filter ? gst_caps_ref (filter) : NULL;
  }

  /* Intersect the listeners' caps, those already receiving buffers first and
   * then those still waiting for caps. A listener whose caps would empty the
   * intersection is detached on its own, so a consumer that can never be
   * served (attached late, or simply misconfigured) does not take the node
   * down for the listeners already running. */
  for (pass = 0; pass < 2; pass++) {
    for (i = 0; i < entries->len; i++) {
      GstInterPipeSinkListenerCaps *entry =
          &g_array_index (entries, GstInterPipeSinkListenerCaps, i);
      GstCaps *candidate;

      if (entry->primed != (pass == 0) || !entry->caps)
        continue;

      candidate = gst_inter_pipe_sink_caps_intersect (entry->caps, negotiated);
      if (gst_caps_is_empty (candidate)) {
        GST_WARNING_OBJECT (sink, "Listener %s caps %" GST_PTR_FORMAT
            " do not intersect the other listeners' %" GST_PTR_FORMAT,
            gst_inter_pipe_ilistener_get_name (entry->listener), entry->caps,
            negotiated);
        evict = g_list_prepend (evict, gst_object_ref (entry->listener));
        gst_caps_unref (candidate);
        continue;
      }
      if (negotiated)
        gst_caps_unref (negotiated);
      negotiated = candidate;
    }
  }

  if (negotiated) {
    GST_INFO_OBJECT (sink, "Caps negotiated: %" GST_PTR_FORMAT, negotiated);
    /* Take into account the upstream caps suggestion. */
    result = gst_inter_pipe_sink_caps_intersect (negotiated, filter);
    GST_INFO_OBJECT (sink, "Filtered caps: %" GST_PTR_FORMAT, result);
    if (gst_caps_is_empty (result)) {
      gst_caps_unref (result);
      result = NULL;
    }
  }

  if (!result) {
    /* Nothing upstream can produce is accepted by the listeners that are
     * left, or none of them reported caps: there is no single listener to
     * blame, so detach them all and start over. */
    GST_ERROR_OBJECT (sink, "Failed to obtain an intersection between "
        "upstream elements and listeners");
    for (i = 0; i < entries->len; i++) {
      GstInterPipeIListener *listener =
          g_array_index (entries, GstInterPipeSinkListenerCaps, i).listener;

      if (!g_list_find (evict, listener))
        evict = g_list_prepend (evict, gst_object_ref (listener));
    }
    if (negotiated) {
      gst_caps_unref (negotiated);
      negotiated = NULL;
    }
  }

  g_mutex_lock (&sink->listeners_mutex);
  gst_caps_replace (&sink->caps_negotiated, negotiated);
  g_mutex_unlock (&sink->listeners_mutex);
  if (negotiated)
    gst_caps_unref (negotiated);

  gst_inter_pipe_sink_free_listener_caps (entries);
  gst_inter_pipe_sink_detach_listeners (sink, evict);

  return result;
}

static gboolean
gst_inter_pipe_sink_set_caps (GstBaseSink * base, GstCaps * caps)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;
  GArray *entries;
  gboolean ret = TRUE;
  gboolean compatible;
  gboolean reconfigure = FALSE;
  guint i;

  sink = GST_INTER_PIPE_SINK (base);
  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);

  if (!GST_BASE_SINK_CLASS (gst_inter_pipe_sink_parent_class)->set_caps (base,
          caps)) {
    GST_WARNING_OBJECT (sink, "Parent rejected caps %" GST_PTR_FORMAT, caps);
    return FALSE;
  }

  GST_INFO_OBJECT (sink, "Incoming Caps: %" GST_PTR_FORMAT, caps);
  GST_INFO_OBJECT (sink, "Negotiated Caps: %" GST_PTR_FORMAT,
      sink->caps_negotiated);

  g_mutex_lock (&sink->listeners_mutex);
  /* No one is listening to me, I can accept caps. Checked under the lock so it
   * cannot race a listener being added concurrently. */
  if (0 == g_hash_table_size (listeners)) {
    g_mutex_unlock (&sink->listeners_mutex);
    goto out;
  }
  compatible = sink->caps_negotiated
      && gst_caps_can_intersect (sink->caps_negotiated, caps);
  g_mutex_unlock (&sink->listeners_mutex);

  if (!compatible) {
    GST_WARNING_OBJECT (sink,
        "There's not caps intersection between node %s and listeners. Caps won't be set",
        sink->node_name);
    ret = FALSE;
    goto out;
  }

  /* These caps were negotiated against the listeners registered when get_caps
   * ran. A listener that attached since then may not be able to take them:
   * do not give them to it, drop its buffers, and have upstream renegotiate
   * with it folded in, rather than fail its pipeline with not-negotiated. */
  entries = gst_inter_pipe_sink_query_listener_caps (sink, FALSE);
  g_mutex_lock (&sink->listeners_mutex);
  g_hash_table_remove_all (sink->awaiting_caps);
  for (i = 0; i < entries->len; i++) {
    GstInterPipeSinkListenerCaps *entry =
        &g_array_index (entries, GstInterPipeSinkListenerCaps, i);

    if (!g_hash_table_contains (listeners, entry->listener))
      continue;

    if (entry->caps && !gst_caps_can_intersect (entry->caps, caps)) {
      GST_INFO_OBJECT (sink, "Caps %" GST_PTR_FORMAT " do not intersect "
          "listener %s caps %" GST_PTR_FORMAT ", dropping its buffers until "
          "upstream renegotiates", caps,
          gst_inter_pipe_ilistener_get_name (entry->listener), entry->caps);
      g_hash_table_add (sink->awaiting_caps, entry->listener);
      reconfigure = TRUE;
      continue;
    }

    GST_LOG_OBJECT (sink, "Setting caps %" GST_PTR_FORMAT " to %s", caps,
        gst_inter_pipe_ilistener_get_name (entry->listener));
    gst_inter_pipe_ilistener_set_caps (entry->listener, caps);
  }
  g_mutex_unlock (&sink->listeners_mutex);
  gst_inter_pipe_sink_free_listener_caps (entries);
  GST_INFO_OBJECT (sink, "Listeners caps updated");

  /* Outside the lock: upstream answers by querying this sink's caps, and
   * get_caps takes the listeners lock. */
  if (reconfigure
      && !gst_pad_push_event (GST_INTER_PIPE_SINK_PAD (sink),
          gst_event_new_reconfigure ()))
    GST_WARNING_OBJECT (sink, "Failed to request upstream renegotiation");

 out:
  if (ret) {
    gst_caps_replace (&sink->caps, caps);
    gst_app_sink_set_caps (GST_APP_SINK (sink), caps);
  }

  return ret;
}

static void
gst_inter_pipe_sink_forward_event (gpointer key, gpointer data,
    gpointer user_data)
{
  GstInterPipeIListener *listener;
  GstInterPipeSink *sink;
  GstEvent *event;
  guint64 basetime;
  gpointer *data_array;

  listener = GST_INTER_PIPE_ILISTENER (data);
  data_array = user_data;
  sink = GST_INTER_PIPE_SINK (data_array[0]);
  event = GST_EVENT (data_array[1]);

  if (GST_EVENT_IS_SERIALIZED (event)) {
    GST_INFO_OBJECT (sink, "Incoming serialized event %s",
        GST_EVENT_TYPE_NAME (event));

    /* Update serial event timestamp */
    GST_EVENT_TIMESTAMP (event) = sink->last_buffer_timestamp;
    GST_INFO_OBJECT (sink, "Event timestamp %" GST_TIME_FORMAT,
        GST_TIME_ARGS (GST_EVENT_TIMESTAMP (event)));
  } else {
    GST_INFO_OBJECT (sink, "Incoming non-serialized event %s",
        GST_EVENT_TYPE_NAME (event));
  }

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_EOS:
    case GST_EVENT_CAPS:
      /*We manage the event with other functions */
      break;
    default:
      basetime = gst_element_get_base_time (GST_ELEMENT (sink));
      gst_inter_pipe_ilistener_push_event (listener, gst_event_ref (event),
          basetime);
      break;
  }
}

static gboolean
gst_inter_pipe_sink_event (GstBaseSink * base, GstEvent * event)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;
  gpointer data_array[2];

  sink = GST_INTER_PIPE_SINK (base);

  /* A flush discards the prerolled buffer without rendering it; the next one
   * prerolled must not be compared against it. */
  if (GST_EVENT_TYPE (event) == GST_EVENT_FLUSH_STOP) {
    GST_OBJECT_LOCK (sink);
    gst_buffer_replace (&sink->preroll_buffer, NULL);
    GST_OBJECT_UNLOCK (sink);
  }

  g_mutex_lock (&sink->listeners_mutex);
  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);

  if (sink->forward_events) {
    data_array[0] = sink;
    data_array[1] = event;
    g_hash_table_foreach (listeners, gst_inter_pipe_sink_forward_event,
        (gpointer) data_array);
  }
  g_mutex_unlock (&sink->listeners_mutex);
  return GST_BASE_SINK_CLASS (gst_inter_pipe_sink_parent_class)->event (base,
      event);
}


struct AllocQueryCtx
{
  GstInterPipeSink *sink;
  GstQuery *query;
  GstAllocationParams params;
  guint size;
  guint min_buffers;
  gboolean first_query;
  guint num_listeners;
};


static gboolean
gst_inter_pipe_sink_forward_query_allocation (gpointer key, gpointer data,
    gpointer user_data)
{
  struct AllocQueryCtx *ctx;
  GstInterPipeIListener *listener;
  gchar *listener_name;
  GstInterPipeSink *sink;
  GstQuery *query;
  GstCaps *caps;
  gboolean ret = TRUE;
  guint count, i, size, min;

  listener = GST_INTER_PIPE_ILISTENER (data);
  listener_name = (gchar *) gst_inter_pipe_ilistener_get_name (listener);
  ctx = user_data;
  sink = ctx->sink;

  GST_DEBUG_OBJECT (sink, "Aggregating allocation from listener %s",
      listener_name);

  gst_query_parse_allocation (ctx->query, &caps, NULL);

  query = gst_query_new_allocation (caps, FALSE);
  if (!gst_inter_pipe_ilistener_query (listener, query)) {
    GST_DEBUG_OBJECT (sink,
        "Allocation query failed on listener %s, ignoring allocation",
        listener_name);
    ret = FALSE;
    goto out;
  }

  /* Allocation Filter, extract of code from tee element */

  /* Allocation Params:
   * store the maximum alignment, prefix and padding, but ignore the
   * allocators and the flags which are tied to downstream allocation*/
  count = gst_query_get_n_allocation_params (query);
  for (i = 0; i < count; i++) {
    GstAllocationParams params = { 0, };

    gst_query_parse_nth_allocation_param (query, i, NULL, &params);

    GST_DEBUG_OBJECT (sink, "Aggregating AllocationParams align=%"
        G_GSIZE_FORMAT " prefix=%" G_GSIZE_FORMAT " padding=%"
        G_GSIZE_FORMAT, params.align, params.prefix, params.padding);

    if (ctx->params.align < params.align)
      ctx->params.align = params.align;

    if (ctx->params.prefix < params.prefix)
      ctx->params.prefix = params.prefix;

    if (ctx->params.padding < params.padding)
      ctx->params.padding = params.padding;
  }

  /* Allocation Pool:
   * We want to keep the biggest size and biggest minimum number of buffers to
   * make sure downstream requirement can be satisfied. We don't really care
   * about the maximum, as this is a parameter of the downstream provided
   * pool. We only read the first allocation pool as the minimum number of
   * buffers is normally constant regardless of the pool being used. */
  if (gst_query_get_n_allocation_pools (query) > 0) {
    gst_query_parse_nth_allocation_pool (query, 0, NULL, &size, &min, NULL);

    GST_DEBUG_OBJECT (sink,
        "Aggregating allocation pool size=%u min_buffers=%u", size, min);

    if (ctx->size < size)
      ctx->size = size;

    if (ctx->min_buffers < min)
      ctx->min_buffers = min;
  }

  /* Allocation Meta:
   * For allocation meta, we'll need to aggregate the argument using the new
   * GstMetaInfo::agggregate_func */
  count = gst_query_get_n_allocation_metas (query);
  for (i = 0; i < count; i++) {
    guint ctx_index;
    GType api;
    const GstStructure *param;

    api = gst_query_parse_nth_allocation_meta (query, i, &param);

    /* For the first query, copy all metas */
    if (ctx->first_query) {
      gst_query_add_allocation_meta (ctx->query, api, param);
      continue;
    }

    /* Afterward, aggregate the common params */
    if (gst_query_find_allocation_meta (ctx->query, api, &ctx_index)) {
      const GstStructure *ctx_param;

      gst_query_parse_nth_allocation_meta (ctx->query, ctx_index, &ctx_param);

      /* Keep meta which has no params */
      if (ctx_param == NULL && param == NULL)
        continue;

      GST_DEBUG_OBJECT (sink, "Dropping allocation meta %s", g_type_name (api));
      gst_query_remove_nth_allocation_meta (ctx->query, ctx_index);
    }
  }

  /* Finally, cleanup metas from the stored query that aren't support on this
   * listener. */
  count = gst_query_get_n_allocation_metas (ctx->query);
  for (i = 0; i < count;) {
    GType api = gst_query_parse_nth_allocation_meta (ctx->query, i, NULL);

    if (!gst_query_find_allocation_meta (query, api, NULL)) {
      GST_DEBUG_OBJECT (sink, "Dropping allocation meta %s", g_type_name (api));
      gst_query_remove_nth_allocation_meta (ctx->query, i);
      count--;
      continue;
    }

    i++;
  }

  ctx->first_query = FALSE;
  ctx->num_listeners++;

out:
  gst_query_unref (query);
  return ret;
}

static gboolean
gst_inter_pipe_sink_stop (GstBaseSink * base)
{
  GstInterPipeSink *sink = GST_INTER_PIPE_SINK (base);

  /* A buffer prerolled but never rendered (flushed, or the pipeline stopped
   * in PAUSED) must not be matched against the next run's buffers. */
  GST_OBJECT_LOCK (sink);
  gst_buffer_replace (&sink->preroll_buffer, NULL);
  GST_OBJECT_UNLOCK (sink);

  return GST_BASE_SINK_CLASS (gst_inter_pipe_sink_parent_class)->stop (base);
}

static void
gst_inter_pipe_sink_notify_latency_changed (gpointer key, gpointer value,
    gpointer user_data)
{
  gst_inter_pipe_ilistener_latency_changed (GST_INTER_PIPE_ILISTENER (value));
}

static void
gst_inter_pipe_sink_latency_changed (GstInterPipeSink * sink)
{
  g_mutex_lock (&sink->listeners_mutex);
  g_hash_table_foreach (GST_INTER_PIPE_SINK_LISTENERS (sink),
      gst_inter_pipe_sink_notify_latency_changed, NULL);
  g_mutex_unlock (&sink->listeners_mutex);
}

/* The bin configures this sink's latency with a LATENCY event; listeners
 * report it downstream as their upstream latency, so tell them. */
static gboolean
gst_inter_pipe_sink_send_event (GstElement * element, GstEvent * event)
{
  GstInterPipeSink *sink = GST_INTER_PIPE_SINK (element);
  gboolean is_latency = GST_EVENT_TYPE (event) == GST_EVENT_LATENCY;
  gboolean ret;

  ret = GST_ELEMENT_CLASS (gst_inter_pipe_sink_parent_class)->send_event
      (element, event);

  if (is_latency)
    gst_inter_pipe_sink_latency_changed (sink);

  return ret;
}

static void
gst_inter_pipe_sink_delay_changed (GObject * object, GParamSpec * pspec,
    gpointer user_data)
{
  gst_inter_pipe_sink_latency_changed (GST_INTER_PIPE_SINK (object));
}

static gboolean
gst_inter_pipe_sink_propose_allocation (GstBaseSink * base, GstQuery * query)
{
  struct AllocQueryCtx ctx = { 0 };
  GstInterPipeSink *sink;
  GHashTable *listeners;
  GHashTableIter iter;
  gboolean ret = TRUE;
  gpointer key, value;

  sink = GST_INTER_PIPE_SINK (base);

  g_mutex_lock (&sink->listeners_mutex);
  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);

  ctx.sink = sink;
  ctx.query = query;
  ctx.first_query = TRUE;
  gst_allocation_params_init (&ctx.params);

  g_hash_table_iter_init (&iter, listeners);

  /* Aggregate every listener; if any listener cannot satisfy the allocation
   * query, fail so the metas are dropped rather than claiming support the
   * listener does not have. (ret |= would stay TRUE forever, leaving the
   * failure handling below dead.) */
  while (g_hash_table_iter_next (&iter, &key, &value)) {
    ret &= gst_inter_pipe_sink_forward_query_allocation (key, value, &ctx);
  }

  if (ret) {
    guint count = gst_query_get_n_allocation_metas (query);
    guint i;

    GST_DEBUG_OBJECT (sink,
        "Final allocation parameters: align=%" G_GSIZE_FORMAT " prefix=%"
        G_GSIZE_FORMAT " padding %" G_GSIZE_FORMAT, ctx.params.align,
        ctx.params.prefix, ctx.params.padding);

    GST_DEBUG_OBJECT (sink, "Final allocation pools: size=%u  min_buffers=%u",
        ctx.size, ctx.min_buffers);

    GST_DEBUG_OBJECT (sink, "Final %u allocation meta:", count);

    for (i = 0; i < count; i++) {
      GST_DEBUG_OBJECT (sink, "    + aggregated allocation meta %s",
          g_type_name (gst_query_parse_nth_allocation_meta (ctx.query, i,
                  NULL)));
    }

    /* Allocate one more buffers when multiplexing so we don't starve the
     * downstream threads. */
    if (ctx.num_listeners > 1)
      ctx.min_buffers++;

    /* Check that we actually have parameters besides the defaults. */
    if (ctx.params.align || ctx.params.prefix || ctx.params.padding) {
      gst_query_add_allocation_param (ctx.query, NULL, &ctx.params);
    }

    /* When size == 0, buffers created from this pool would have no memory
     * allocated. */
    if (ctx.size) {
      gst_query_add_allocation_pool (ctx.query, NULL, ctx.size,
          ctx.min_buffers, 0);
    }

  } else {
    guint count = gst_query_get_n_allocation_metas (query);
    guint i;

    for (i = 1; i <= count; i++) {
      gst_query_remove_nth_allocation_meta (query, count - i);
    }
  }

  g_mutex_unlock (&sink->listeners_mutex);

  return ret;
}

/* Appsink Callbacks */
typedef struct
{
  GstInterPipeSink *sink;
  GstBuffer *buffer;
  GstCaps *caps;
  /* Caps of the listeners still waiting for caps, queried before taking
   * listeners_mutex. NULL when every listener was primed. */
  GArray *unprimed;
  gboolean reconfigure;
  gboolean all_accepted;
} GstInterPipeSinkFanOut;

static void
gst_inter_pipe_sink_push_to_listener (gpointer key, gpointer data,
    gpointer user_data)
{
  GstInterPipeSinkFanOut *fan_out = user_data;
  GstInterPipeSink *sink = fan_out->sink;
  GstInterPipeIListener *listener;
  const gchar *listener_name;
  guint64 basetime;

  listener = GST_INTER_PIPE_ILISTENER (data);
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  if (g_hash_table_contains (sink->awaiting_caps, listener)) {
    GST_LOG_OBJECT (sink, "Dropping buffer for listener %s until upstream "
        "renegotiates", listener_name);
    fan_out->all_accepted = FALSE;
    return;
  }

  /* Guarantee caps reach the listener before its first buffer. A listener
   * that attached before this node had caps (e.g. a consumer pipeline started
   * before a slow/network producer began producing) is otherwise pushed a
   * buffer with no caps set on its appsrc, which fails downstream
   * negotiation (not-negotiated). When the listener has no caps yet, set them
   * from the current sample's caps first, but only caps its downstream
   * accepts; otherwise drop its buffers and have upstream renegotiate with it
   * folded in. Listeners that already have caps are left untouched, so
   * allow-renegotiation behaviour is preserved.
   *
   * is_negotiated is a cheap flag that resets on every (re)attach, so the
   * caps query this needs is only paid while a listener is unprimed. */
  if (fan_out->caps && !gst_inter_pipe_ilistener_is_negotiated (listener)) {
    const GstInterPipeSinkListenerCaps *entry =
        gst_inter_pipe_sink_find_listener_caps (fan_out->unprimed, listener);

    if (!entry) {
      /* Attached after the caps check: decide on the next buffer. */
      fan_out->all_accepted = FALSE;
      return;
    }
    if (entry->caps && !gst_caps_can_intersect (entry->caps, fan_out->caps)) {
      GST_DEBUG_OBJECT (sink, "Node caps %" GST_PTR_FORMAT " do not intersect "
          "listener %s caps %" GST_PTR_FORMAT ", dropping its buffers until "
          "upstream renegotiates", fan_out->caps, listener_name, entry->caps);
      fan_out->reconfigure = TRUE;
      fan_out->all_accepted = FALSE;
      return;
    }

    GST_INFO_OBJECT (sink, "Listener %s has no caps yet; applying node caps "
        "%" GST_PTR_FORMAT " before its first buffer", listener_name,
        fan_out->caps);
    gst_inter_pipe_ilistener_set_caps (listener, fan_out->caps);
  }

  GST_LOG_OBJECT (sink, "Forwarding buffer %p to %s", fan_out->buffer,
      listener_name);

  basetime = gst_element_get_base_time (GST_ELEMENT (sink));
  if (!gst_inter_pipe_ilistener_push_buffer (listener,
          gst_buffer_ref (fan_out->buffer), basetime)) {
    GST_DEBUG_OBJECT (sink, "Listener %s did not accept the buffer",
        listener_name);
    fan_out->all_accepted = FALSE;
  }
}

/* Forward the sample's buffer to every listener. Takes ownership of the
 * sample. Returns TRUE if there was at least one listener and every one of
 * them took the buffer. */
static gboolean
gst_inter_pipe_sink_process_sample (GstInterPipeSink * sink, GstSample * sample)
{
  GstInterPipeSinkFanOut fan_out = { NULL, };
  GHashTableIter iter;
  gpointer value;
  gboolean any_unprimed = FALSE;
  guint num_listeners;

  fan_out.sink = sink;
  fan_out.buffer = gst_sample_get_buffer (sample);
  if (!fan_out.buffer) {
    GST_LOG_OBJECT (sink, "Sample carries no buffer, nothing to forward");
    gst_sample_unref (sample);
    return FALSE;
  }
  /* Sample carries the negotiated caps; push_to_listener uses them to set caps
   * on any listener that attached before this node had caps. */
  fan_out.caps = gst_sample_get_caps (sample);
  fan_out.all_accepted = TRUE;

  /* Unprimed listeners need a caps query before they get caps, which must not
   * run under listeners_mutex (see query_listener_caps). Only pay for it while
   * some listener is unprimed. */
  if (fan_out.caps) {
    g_mutex_lock (&sink->listeners_mutex);
    g_hash_table_iter_init (&iter, GST_INTER_PIPE_SINK_LISTENERS (sink));
    while (!any_unprimed && g_hash_table_iter_next (&iter, NULL, &value))
      any_unprimed = !gst_inter_pipe_ilistener_is_negotiated (value);
    g_mutex_unlock (&sink->listeners_mutex);
    if (any_unprimed)
      fan_out.unprimed = gst_inter_pipe_sink_query_listener_caps (sink, TRUE);
  }

  g_mutex_lock (&sink->listeners_mutex);

  /* Update last_buffer_timestamp */
  sink->last_buffer_timestamp = GST_BUFFER_PTS (fan_out.buffer);

  GST_LOG_OBJECT (sink, "Received new buffer %p on node %s", fan_out.buffer,
      sink->node_name);

  num_listeners = g_hash_table_size (GST_INTER_PIPE_SINK_LISTENERS (sink));
  g_hash_table_foreach (GST_INTER_PIPE_SINK_LISTENERS (sink),
      gst_inter_pipe_sink_push_to_listener, &fan_out);

  g_mutex_unlock (&sink->listeners_mutex);

  if (fan_out.unprimed)
    gst_inter_pipe_sink_free_listener_caps (fan_out.unprimed);
  gst_sample_unref (sample);

  /* Outside the lock: upstream may answer a reconfigure by querying this
   * sink's caps, and get_caps takes the listeners lock. */
  if (fan_out.reconfigure
      && !gst_pad_push_event (GST_INTER_PIPE_SINK_PAD (sink),
          gst_event_new_reconfigure ()))
    GST_WARNING_OBJECT (sink, "Failed to request upstream renegotiation");

  return num_listeners > 0 && fan_out.all_accepted;
}

static GstFlowReturn
gst_inter_pipe_sink_new_buffer (GstAppSink * asink, gpointer data)
{
  GstInterPipeSink *sink;
  GstSample *sample;
  GstBuffer *buffer;
  gboolean already_forwarded;

  sink = GST_INTER_PIPE_SINK (asink);

  sample = gst_app_sink_pull_sample (asink);
  if (!sample)
    return GST_FLOW_OK;

  /* The buffer new_preroll forwarded while the pipeline was staying paused is
   * rendered again once it resumes; listeners already have it. */
  buffer = gst_sample_get_buffer (sample);
  GST_OBJECT_LOCK (sink);
  already_forwarded = buffer && buffer == sink->preroll_buffer;
  gst_buffer_replace (&sink->preroll_buffer, NULL);
  GST_OBJECT_UNLOCK (sink);

  if (already_forwarded) {
    GST_LOG_OBJECT (sink, "Buffer %p was forwarded at preroll, not forwarding "
        "it again", buffer);
    gst_sample_unref (sample);
    return GST_FLOW_OK;
  }

  gst_inter_pipe_sink_process_sample (sink, sample);

  return GST_FLOW_OK;
}


/* Whether the pipeline this sink belongs to is headed for a state below
 * PLAYING, so the buffer prerolled now will not be rendered soon. */
static gboolean
gst_inter_pipe_sink_staying_paused (GstInterPipeSink * sink)
{
  GstObject *top;
  GstObject *parent;
  GstState target;

  top = gst_object_ref (GST_OBJECT (sink));
  while ((parent = gst_object_get_parent (top)) != NULL) {
    gst_object_unref (top);
    top = parent;
  }

  GST_OBJECT_LOCK (top);
  target = GST_STATE_TARGET (top);
  GST_OBJECT_UNLOCK (top);
  gst_object_unref (top);

  return target < GST_STATE_PLAYING;
}

static GstFlowReturn
gst_inter_pipe_sink_new_preroll (GstAppSink * asink, gpointer data)
{
  GstInterPipeSink *sink;
  GstSample *sample;
  GstBuffer *buffer;

  sink = GST_INTER_PIPE_SINK (asink);

  sample = gst_app_sink_pull_preroll (asink);
  if (!sample)
    return GST_FLOW_OK;

  /* GstBaseSink prerolls a buffer before waiting for its clock time, and then
   * renders the same buffer once that time comes. The render is the on-time
   * hand-over, made with the pipeline PLAYING and its base time valid, which
   * compensate-ts relies on. So forward at render, except when the pipeline
   * is staying paused and will not render: forward the preroll buffer then,
   * so listeners get it, and remember it so a later resume does not deliver
   * it twice. If any listener did not take it, leave it to the render. */
  if (!gst_inter_pipe_sink_staying_paused (sink)) {
    gst_sample_unref (sample);
    return GST_FLOW_OK;
  }

  buffer = gst_sample_get_buffer (sample);
  if (buffer)
    gst_buffer_ref (buffer);

  if (gst_inter_pipe_sink_process_sample (sink, sample) && buffer) {
    GST_OBJECT_LOCK (sink);
    gst_buffer_replace (&sink->preroll_buffer, buffer);
    GST_OBJECT_UNLOCK (sink);
  }

  if (buffer)
    gst_buffer_unref (buffer);

  return GST_FLOW_OK;
}

static void
gst_inter_pipe_sink_send_eos (gpointer key, gpointer data, gpointer user_data)
{
  GstInterPipeSink *sink;
  GstInterPipeIListener *listener;
  gchar *listener_name;

  sink = GST_INTER_PIPE_SINK (user_data);
  listener = GST_INTER_PIPE_ILISTENER (data);
  listener_name = (gchar *) gst_inter_pipe_ilistener_get_name (listener);

  GST_LOG_OBJECT (sink, "Forwarding EOS to %s", listener_name);

  gst_inter_pipe_ilistener_send_eos (listener);
}

static void
gst_inter_pipe_sink_eos (GstAppSink * asink, gpointer data)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;

  sink = GST_INTER_PIPE_SINK (asink);

  g_mutex_lock (&sink->listeners_mutex);
  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);

  GST_LOG_OBJECT (sink, "Received new EOS on node %s", sink->node_name);

  if (sink->forward_eos) {
    g_hash_table_foreach (listeners, gst_inter_pipe_sink_send_eos,
        (gpointer) sink);
  } else {
    GST_LOG_OBJECT (sink, "Ignoring EOS");
  }
  g_mutex_unlock (&sink->listeners_mutex);
}

/* GstInterPipeINode interface implementation */
static void
gst_inter_pipe_inode_init (GstInterPipeINodeInterface * iface)
{
  iface->add_listener = gst_inter_pipe_sink_add_listener;
  iface->remove_listener = gst_inter_pipe_sink_remove_listener;
  iface->receive_event = gst_inter_pipe_sink_receive_event;
}

static gboolean
gst_inter_pipe_sink_add_listener (GstInterPipeINode * iface,
    GstInterPipeIListener * listener)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;
  const gchar *listener_name;
  GstCaps *srccaps, *sinkcaps;
  gboolean src_negotiated;
  gboolean reconfigure = FALSE;

  g_return_val_if_fail (iface, FALSE);
  g_return_val_if_fail (listener, FALSE);

  sink = GST_INTER_PIPE_SINK (iface);

  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  GST_INFO_OBJECT (sink, "Adding new listener %s", listener_name);

  /* Check caps before add listener */
  srccaps = gst_inter_pipe_ilistener_get_caps (listener, &src_negotiated);
  sinkcaps = gst_app_sink_get_caps (GST_APP_SINK (sink));

  if (src_negotiated) {
    gboolean has_listeners;
    gboolean has_negotiated_caps;

    if (!sinkcaps) {
      /* srccaps was taken with a ref by get_caps; the add_to_list path skips
       * the unrefs below, so release it here before jumping. */
      if (srccaps)
        gst_caps_unref (srccaps);
      goto add_to_list;
    }

    /* Snapshot the shared state under the lock so the negotiation decision is
     * made on a consistent view. The lock is not held across the pad and caps
     * calls below, which would risk re-entering this element's locks. */
    g_mutex_lock (&sink->listeners_mutex);
    has_listeners = 0 != g_hash_table_size (listeners);
    has_negotiated_caps = sink->caps_negotiated != NULL;
    g_mutex_unlock (&sink->listeners_mutex);

    if (!has_negotiated_caps && !has_listeners
        && !gst_caps_is_equal (srccaps, sinkcaps)) {

      if (!gst_pad_push_event (GST_INTER_PIPE_SINK_PAD (sink),
              gst_event_new_reconfigure ()))
        goto reconfigure_event_error;

      GST_INFO_OBJECT (sink, "Reconfigure event sent correctly");
    }

    if (has_negotiated_caps && has_listeners
        && !gst_caps_is_equal (srccaps, sinkcaps)) {

      if (!gst_inter_pipe_sink_are_caps_compatible (sink, srccaps, sinkcaps))
        goto renegotiate_error;

      if (!gst_inter_pipe_ilistener_set_caps (listener, sinkcaps))
        goto set_caps_failed;
    }
  } else {
    /* The listener has no caps yet. Prime it with the node's current caps
     * when its downstream can take them. Otherwise leave it unprimed and have
     * upstream renegotiate once the listener is registered: get_caps then
     * folds its caps into the negotiation and set_caps delivers the result.
     * Caps the listener cannot accept would only fail its pipeline with
     * not-negotiated. */
    GstEvent *capsev = gst_pad_get_sticky_event (GST_INTER_PIPE_SINK_PAD (sink),
        GST_EVENT_CAPS, 0);
    if (capsev) {
      GstCaps *caps;
      gst_event_parse_caps (capsev, &caps);
      if (!srccaps || gst_caps_can_intersect (srccaps, caps)) {
        GST_INFO_OBJECT (sink, "Setting listener caps to %" GST_PTR_FORMAT,
            caps);
        gst_inter_pipe_ilistener_set_caps (listener, caps);
      } else {
        GST_INFO_OBJECT (sink, "Node caps %" GST_PTR_FORMAT " do not intersect "
            "listener %s caps %" GST_PTR_FORMAT ", renegotiating upstream",
            caps, listener_name, srccaps);
        reconfigure = TRUE;
      }
      gst_event_unref (capsev);
    } else {
      GST_INFO_OBJECT (sink,
          "Cannot set caps, no caps event stuck on sink pad");
    }
  }

  if (srccaps)
    gst_caps_unref (srccaps);
  if (sinkcaps)
    gst_caps_unref (sinkcaps);

add_to_list:
  g_mutex_lock (&sink->listeners_mutex);
  /* Key by the listener object: its pointer is stable for its lifetime,
   * whereas its name pointer could change if the element were renamed. */
  if (g_hash_table_contains (listeners, listener))
    goto already_registered;

  g_hash_table_insert (listeners, (gpointer) listener, (gpointer) listener);

  g_mutex_unlock (&sink->listeners_mutex);

  /* Only now that the listener is in the table: the renegotiation this
   * triggers runs get_caps on the producer's streaming thread, which has to
   * see the new listener to fold its caps in. */
  if (reconfigure
      && !gst_pad_push_event (GST_INTER_PIPE_SINK_PAD (sink),
          gst_event_new_reconfigure ()))
    GST_WARNING_OBJECT (sink, "Failed to request upstream renegotiation "
        "for listener %s", listener_name);

  return TRUE;

/* Errors */
renegotiate_error:
  {
    GST_ERROR_OBJECT (sink, "Can not connect listener, caps do not intersect");
    goto error;
  }
reconfigure_event_error:
  {
    GST_ERROR_OBJECT (sink, "Failed to reconfigure");
    goto error;
  }
set_caps_failed:
  {
    GST_ERROR_OBJECT (sink, "Failed to set caps to listener %s", listener_name);
    goto error;

  }
already_registered:
  {
    GST_WARNING_OBJECT (sink, "Listener %s already registered in node %s",
        listener_name, GST_OBJECT_NAME (sink));
    g_mutex_unlock (&sink->listeners_mutex);

    return TRUE;
  }
error:
  {
    if (srccaps)
      gst_caps_unref (srccaps);
    if (sinkcaps)
      gst_caps_unref (sinkcaps);
    return FALSE;
  }
}

static gboolean
gst_inter_pipe_sink_remove_listener (GstInterPipeINode * iface,
    GstInterPipeIListener * listener)
{
  GstInterPipeSink *sink;
  GHashTable *listeners;
  const gchar *listener_name;

  sink = GST_INTER_PIPE_SINK (iface);
  g_mutex_lock (&sink->listeners_mutex);

  listeners = GST_INTER_PIPE_SINK_LISTENERS (sink);
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  GST_INFO_OBJECT (sink, "Removing listener %s", listener_name);

  if (!g_hash_table_remove (listeners, listener))
    goto not_registered;
  g_hash_table_remove (sink->awaiting_caps, listener);

  if (0 == g_hash_table_size (listeners) && sink->caps_negotiated) {
    gst_caps_unref (sink->caps_negotiated);
    sink->caps_negotiated = NULL;
  }
  g_mutex_unlock (&sink->listeners_mutex);

  return TRUE;

not_registered:
  {
    GST_ERROR_OBJECT (sink, "Listener %s is not registered in node %s",
        listener_name, GST_OBJECT_NAME (sink));
    g_mutex_unlock (&sink->listeners_mutex);
    return FALSE;
  }
}

static gboolean
gst_inter_pipe_sink_receive_event (GstInterPipeINode * iface, GstEvent * event)
{
  GstInterPipeSink *self;
  GstPad *sinkpad;
  const GstStructure *structure;
  gboolean is_force_key_unit;

  self = GST_INTER_PIPE_SINK (iface);

  /* Only the force-key-unit request crosses the node boundary: it is about
   * stream content, which producer and consumers share, and broadcasting it
   * is safe with any listener count (an extra keyframe costs a little
   * bitrate and unblocks a freshly attached consumer). Every other upstream
   * event type is pipeline-local — clocks (QOS, LATENCY), caps
   * (RECONFIGURE), or playback position (SEEK) — and does not translate
   * across a boundary whose caps are frozen and whose buffer timestamps are
   * rebased per listener; a consumer-originated RECONFIGURE in particular
   * has deadlocked the producer's source in renegotiation. Dropped events
   * return TRUE: the node has accepted and consumed them. */
  structure = gst_event_get_structure (event);
  is_force_key_unit = GST_EVENT_TYPE (event) == GST_EVENT_CUSTOM_UPSTREAM
      && structure != NULL
      && gst_structure_has_name (structure, "GstForceKeyUnit");

  if (!is_force_key_unit) {
    GST_DEBUG_OBJECT (self,
        "Dropping upstream %s event: only force-key-unit crosses the "
        "interpipe boundary", GST_EVENT_TYPE_NAME (event));
    gst_event_unref (event);
    return TRUE;
  }

  sinkpad = GST_INTER_PIPE_SINK_PAD (self);
  return gst_pad_push_event (sinkpad, event);
}
