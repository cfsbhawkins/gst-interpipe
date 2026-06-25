/* GStreamer
 * Copyright (C) 2013-2016 Michael Grüner <michael.gruner@ridgerun.com>
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>
#include <string.h>

#include "gstinterpipe.h"

/**
 * SECTION:gstinterpipe
 *
 * GstInterpipe Core handling inter pipeline communication. 
 */

GST_DEBUG_CATEGORY (gst_inter_pipe_debug);
#define GST_CAT_DEFAULT gst_inter_pipe_debug

typedef struct _GstInterPipeListenerPriv GstInterPipeListenerPriv;
struct _GstInterPipeListenerPriv
{
  GstInterPipeIListener *listener;
  /* Owned copy of the node name this listener is attached to. Duplicated on
   * store and freed on replace/remove so it never depends on the listener's
   * own listen-to string staying alive (which a concurrent property set could
   * free). */
  gchar *listen_to;
};

/* Global mutexes for singletons */
static GRecMutex listeners_mutex;
static GMutex nodes_mutex;

static GHashTable *gst_inter_pipe_get_listeners ();
static GHashTable *gst_inter_pipe_get_nodes ();
static void gst_inter_pipe_notify_node_added (gpointer listener_name,
    gpointer _listener, gpointer data);
static void gst_inter_pipe_notify_node_removed (gpointer _listener_name,
    gpointer _listener, gpointer data);
static gboolean gst_inter_pipe_leave_listeners_table (GstInterPipeIListener *
    listener);
static gboolean gst_inter_pipe_leave_node_priv (GstInterPipeIListener *
    listener);

static GHashTable *
gst_inter_pipe_get_listeners (void)
{
  /* The listeners singleton */
  static GHashTable *gst_inter_pipe_listeners = NULL;

  if (!gst_inter_pipe_listeners) {
    /* Own the key strings so the table never depends on the listener's name
     * pointer (GST_OBJECT_NAME) staying alive: a rename frees the old name
     * buffer, which would otherwise leave a dangling key. */
    gst_inter_pipe_listeners =
        g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
  }
  return gst_inter_pipe_listeners;
}

/* Value-destroy for the nodes table: clear and free the GWeakRef box. */
static void
gst_inter_pipe_node_weak_ref_free (gpointer data)
{
  GWeakRef *weak = (GWeakRef *) data;

  g_weak_ref_clear (weak);
  g_free (weak);
}

static GHashTable *
gst_inter_pipe_get_nodes (void)
{
  /* The nodes singleton */
  static GHashTable *gst_inter_pipe_nodes = NULL;

  if (!gst_inter_pipe_nodes) {
    /* Own the key strings so the table never depends on the node's name
     * outliving its entry. Values are GWeakRef boxes (not borrowed strong
     * pointers) so a lookup can never resurrect a finalizing node — see
     * gst_inter_pipe_get_node. */
    gst_inter_pipe_nodes =
        g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
        gst_inter_pipe_node_weak_ref_free);
  }
  return gst_inter_pipe_nodes;
}

GstInterPipeINode *
gst_inter_pipe_get_node (const gchar * node_name)
{
  GHashTable *nodes;
  GWeakRef *weak;
  GstInterPipeINode *value = NULL;

  g_return_val_if_fail (node_name != NULL, NULL);

  g_mutex_lock (&nodes_mutex);
  nodes = gst_inter_pipe_get_nodes ();

  /* The table holds a GWeakRef, not a borrowed strong pointer. g_weak_ref_get
   * atomically returns a new strong reference, or NULL if the node has already
   * reached refcount 0 / is being finalized. A plain gst_object_ref on a
   * borrowed pointer could instead *resurrect* a node that gstd is concurrently
   * tearing down (refcount 0 -> 1 -> 0 = double finalize), after which gstd's
   * own set_state/bin_remove would operate on freed memory and trip the
   * GST_IS_ELEMENT cast assertions. */
  weak = (GWeakRef *) g_hash_table_lookup (nodes, node_name);
  if (weak)
    value = (GstInterPipeINode *) g_weak_ref_get (weak);
  g_mutex_unlock (&nodes_mutex);

  return value;
}

gboolean
gst_inter_pipe_listen_node (GstInterPipeIListener * listener,
    const gchar * node_name)
{
  GstInterPipeINode *node;
  GstInterPipeListenerPriv *listener_priv;
  GHashTable *listeners;
  const gchar *listener_name;
  gboolean priv_is_new = FALSE;

  g_return_val_if_fail (listener != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_rec_mutex_lock (&listeners_mutex);

  listeners = gst_inter_pipe_get_listeners ();
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  GST_INFO ("listener %s listen to node %s", listener_name, node_name);
  if (g_hash_table_contains (listeners, listener_name)) {
    /*TODO: check if listener is the same listener from the list? */
    listener_priv =
        (GstInterPipeListenerPriv *) g_hash_table_lookup (listeners,
        listener_name);
    if (!g_strcmp0 (listener_priv->listen_to, node_name))
      goto already_listen;

    if (listener_priv->listen_to)
      gst_inter_pipe_leave_node_priv (listener);

  } else {
    listener_priv = g_malloc (sizeof (GstInterPipeListenerPriv));
    listener_priv->listener = listener;
    listener_priv->listen_to = NULL;
    priv_is_new = TRUE;
  }

  GST_INFO ("Adding new listener %s to node %s", listener_name, node_name);

  node = gst_inter_pipe_get_node (node_name);

  /* If the node is not in the list we will notify later
     when it connects */
  if (node == NULL) {
    GST_INFO ("Node is not available yet, connecting later.");
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
  } else {
    if (!gst_inter_pipe_inode_add_listener (node, listener))
      goto add_failed;
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = g_strdup (node_name);
    gst_object_unref (node);
  }

  /* The table owns a duplicated key (freed by its key-destroy func). On a
   * re-insert with an existing key g_hash_table_insert frees the passed
   * duplicate and keeps the original, so this never leaks. */
  g_hash_table_insert (listeners, g_strdup (listener_name),
      (gpointer) listener_priv);

  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;
already_listen:
  {
    GST_INFO ("Already listening to node %s", node_name);
    g_rec_mutex_unlock (&listeners_mutex);
    return TRUE;
  }
add_failed:
  {
    GST_WARNING ("Could not add listener %s to node %s", listener_name,
        node_name);
    /* We reach here only from the branch that holds a node reference. */
    gst_object_unref (node);
    /* A freshly allocated priv was never inserted into the table, so free it
     * here. An existing priv is owned by the table and left in place. */
    if (priv_is_new)
      g_free (listener_priv);
    g_rec_mutex_unlock (&listeners_mutex);
    return FALSE;
  }
}

static gboolean
gst_inter_pipe_leave_listeners_table (GstInterPipeIListener * listener)
{
  GstInterPipeListenerPriv *listener_priv;
  GHashTable *listeners;
  const gchar *listener_name;

  listeners = gst_inter_pipe_get_listeners ();
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  listener_priv = g_hash_table_lookup (listeners, listener_name);
  if (!g_hash_table_remove (listeners, listener_name))
    return FALSE;

  g_free (listener_priv->listen_to);
  g_free (listener_priv);

  return TRUE;
}

static gboolean
gst_inter_pipe_leave_node_priv (GstInterPipeIListener * listener)
{
  GHashTable *listeners;
  GstInterPipeINode *node;
  GstInterPipeListenerPriv *listener_priv;
  const gchar *listener_name;

  g_return_val_if_fail (listener != NULL, FALSE);

  listeners = gst_inter_pipe_get_listeners ();
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  listener_priv =
      (GstInterPipeListenerPriv *) g_hash_table_lookup (listeners,
      listener_name);
  if (!listener_priv)
    goto no_listener;

  if (listener_priv->listen_to) {
    GST_INFO ("listener %s leaving node %s", listener_name,
        listener_priv->listen_to);

    node = gst_inter_pipe_get_node (listener_priv->listen_to);
    if (node == NULL)
      goto no_node;

    if (!gst_inter_pipe_inode_remove_listener (node, listener))
      goto remove_error;

    gst_object_unref (node);
  }

  return TRUE;

no_listener:
  {
    GST_WARNING ("Listener is not in the connected listeners list");
    return FALSE;
  }
no_node:
  {
    GST_WARNING ("Node %s not found. Could not leave node.",
        listener_priv->listen_to);
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
    return FALSE;
  }
remove_error:
  {
    GST_WARNING
        ("The listener %s was not listening to %s, there's something very wrong",
        listener_name, listener_priv->listen_to);
    gst_object_unref (node);
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
    return FALSE;
  }

}

gboolean
gst_inter_pipe_leave_node (GstInterPipeIListener * listener)
{
  gboolean ret = TRUE;

  g_rec_mutex_lock (&listeners_mutex);

  ret = gst_inter_pipe_leave_node_priv (listener);
  if (!ret)
    goto out;

  if (!gst_inter_pipe_leave_listeners_table (listener))
    goto list_error;

out:
  g_rec_mutex_unlock (&listeners_mutex);
  return ret;

list_error:
  {
    GST_WARNING ("Could not leave node");
    g_rec_mutex_unlock (&listeners_mutex);
    return FALSE;
  }
}

static void
gst_inter_pipe_notify_node_added (gpointer listener_name, gpointer _listener,
    gpointer data)
{
  GstInterPipeListenerPriv *listener_priv = _listener;
  GstInterPipeIListener *listener = listener_priv->listener;
  gchar *node_name = data;

  GST_INFO ("Notifying new node added: %s", node_name);

  gst_inter_pipe_ilistener_node_added (listener, node_name);
}

gboolean
gst_inter_pipe_add_node (GstInterPipeINode * node, const gchar * node_name)
{
  GHashTable *nodes;
  GHashTable *listeners;

  g_return_val_if_fail (node != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_mutex_lock (&nodes_mutex);

  nodes = gst_inter_pipe_get_nodes ();
  if (g_hash_table_contains (nodes, node_name))
    goto no_unique;

  GST_INFO ("Adding node %s", node_name);

  {
    /* Store a GWeakRef box rather than a borrowed strong pointer, so a
     * concurrent gst_inter_pipe_get_node can never resurrect this node while
     * it is being finalized. The table still does not keep the node alive.
     *
     * The g_hash_table_contains check above (under the same lock) guarantees a
     * unique key, so g_hash_table_insert always returns TRUE and takes
     * ownership of the box via the value-destroy func. We deliberately don't
     * clean up on a FALSE return: that would mean the key already existed, in
     * which case GLib has already inserted this box as the value and destroyed
     * the old one, so freeing it here would leave a dangling pointer behind. */
    GWeakRef *weak = g_new0 (GWeakRef, 1);
    g_weak_ref_init (weak, node);
    g_hash_table_insert (nodes, g_strdup (node_name), (gpointer) weak);
  }

  g_mutex_unlock (&nodes_mutex);

  g_rec_mutex_lock (&listeners_mutex);
  listeners = gst_inter_pipe_get_listeners ();
  g_hash_table_foreach (listeners, gst_inter_pipe_notify_node_added,
      (gpointer) node_name);
  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;

no_unique:
  {
    GST_WARNING ("Could not add node %s, it is not unique.", node_name);
    g_mutex_unlock (&nodes_mutex);
    return FALSE;
  }
}

static void
gst_inter_pipe_notify_node_removed (gpointer _listener_name, gpointer _listener,
    gpointer data)
{
  gchar *node_name = data;
  GstInterPipeListenerPriv *listener_priv = _listener;
  GstInterPipeIListener *listener = listener_priv->listener;

  GST_INFO ("Notifying node removed: %s", node_name);

  gst_inter_pipe_ilistener_node_removed (listener, node_name);
}

gboolean
gst_inter_pipe_remove_node (GstInterPipeINode * node, const gchar * node_name)
{
  GHashTable *nodes;
  GHashTable *listeners;

  g_return_val_if_fail (node != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_mutex_lock (&nodes_mutex);

  nodes = gst_inter_pipe_get_nodes ();
  GST_INFO ("Removing node %s", node_name);
  if (!g_hash_table_remove (nodes, (gconstpointer) node_name)) {
    GST_WARNING ("Node %s not found. Could not remove it.", node_name);
    g_mutex_unlock (&nodes_mutex);
    return FALSE;
  }
  g_mutex_unlock (&nodes_mutex);

  g_rec_mutex_lock (&listeners_mutex);
  listeners = gst_inter_pipe_get_listeners ();
  g_hash_table_foreach (listeners, gst_inter_pipe_notify_node_removed,
      (gpointer) node_name);
  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;
}
