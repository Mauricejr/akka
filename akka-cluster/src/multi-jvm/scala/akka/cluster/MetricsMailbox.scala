/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import com.typesafe.config.Config
import akka.actor.{ ActorContext, ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import akka.dispatch.{ Envelope, MailboxType, MessageQueue, UnboundedMailbox, UnboundedQueueBasedMessageQueue }
import akka.event.Logging

object MetricsMailboxExtension extends ExtensionId[MetricsMailboxExtension] with ExtensionIdProvider {
  def lookup = this
  def createExtension(s: ExtendedActorSystem) = new MetricsMailboxExtension(s)
}

class MetricsMailboxExtension(val system: ExtendedActorSystem) extends Extension {
  private val mailboxes = new ConcurrentHashMap[ActorRef, MetricsMailbox]

  def register(actorRef: ActorRef, mailbox: MetricsMailbox): Unit =
    mailboxes.put(actorRef, mailbox)

  def unregister(actorRef: ActorRef): Unit = mailboxes.remove(actorRef)

  def mailboxSize(ref: ActorRef): Int =
    mailboxes.get(ref) match {
      case null    ⇒ 0
      case mailbox ⇒ mailbox.numberOfMessages
    }
}

class MetricsMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  override def create(owner: Option[ActorRef], system: Option[ActorSystem]) = (owner, system) match {
    case (Some(o), Some(s)) ⇒
      val limit = config.getInt("size-limit")
      val mailbox = new MetricsMailbox(o, s, limit)
      MetricsMailboxExtension(s).register(o, mailbox)
      mailbox
    case _ ⇒ throw new Exception("no mailbox owner or system given")
  }
}

class MetricsMailbox(owner: ActorRef, system: ActorSystem, sizeLimit: Int) extends UnboundedMailbox.MessageQueue {

  private val queueSize = new AtomicInteger
  lazy private val m = new Metrics
  class Metrics {
    val log = Logging(system, classOf[MetricsMailbox])
    val path = owner.path.toString
    @volatile var logTime: Long = 0L
  }

  override def dequeue(): Envelope = {
    val x = super.dequeue()
    if (x ne null) {
      val size = queueSize.decrementAndGet
      logSize(size)
    }
    x
  }

  override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
    super.enqueue(receiver, handle)
    val size = queueSize.incrementAndGet
    logSize(size)
  }

  def logSize(size: Int): Unit =
    if (size >= sizeLimit) {
      val now = System.currentTimeMillis
      if (now - m.logTime > 1000) {
        m.logTime = now
        m.log.info("Mailbox [{}] size [{}]", m.path, size)
      }
    }

  override def numberOfMessages: Int = queueSize.get

  override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MetricsMailboxExtension(system).unregister(owner)
  }
}