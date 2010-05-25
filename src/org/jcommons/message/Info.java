package org.jcommons.message;

/**
 * An informational message.
 */
public class Info
  extends AbstractMessage
{
  /** Create an empty message. */
  public Info() {
    this(null);
  }

  /**
   * Create an informational message with the given text.
   *
   * @param text the message text
   */
  public Info(final String text) {
    super();
    setText(text);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isError() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isInfo() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isWarning() {
    return false;
  }
}
