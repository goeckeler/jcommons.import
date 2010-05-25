package org.jcommons.message;

import java.util.List;

/**
 * A message transports some sort of information as a warning, as an error, and so on.
 *
 * For front end validation the appropriate frameworks shall be chosen, these messages are more for administrators,
 * batches and back end processes. They are still lacking internationalization.
 *
 * If some of these methods sound weird, think of messages being chained, and for concrete messages the container
 * methods act always on itself.
 *
 * @author Thorsten Goeckeler
 */
public interface Message
{
  /** @return the current message text, never null */
  String getText();

  /** @return the current error message texts, never null */
  String getFaults();

  /** @return the current warning message texts, never null */
  String getWarnings();

  /** @return the current informational message texts, never null */
  String getInfos();

  /** @return all concrete message texts, never null */
  List<? extends Message> getTexts();

  /** @return all message texts, never null */
  List<? extends Message> getMessages();

  /** @return true if this message indicates at least one error */
  boolean isError();

  /** @return true if this message indicates at least one warning */
  boolean isWarning();

  /** @return true if this message has at least one informational message */
  boolean isInfo();

  /** @return true if no messages are carried at all */
  boolean isEmpty();

  /** @return true if this is a composite message and false if it is a concrete message */
  boolean isComposite();

  /**
   * Adds the given message to the list of messages.
   *
   * @param text the message to add to all messages
   * @return this to allow chaining
   */
  Message add(Message text);

  /**
   * Removes the given message from the list of messages.
   *
   * @param text the message to remove from all messages
   * @return this to allow chaining
   */
  Message remove(Message text);

  /**
   * Clears all messages.
   *
   * @return this to allow chaining
   */
  Message clear();
}
