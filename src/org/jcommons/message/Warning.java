package org.jcommons.message;

/**
 * A warning message.
 */
public class Warning
  extends AbstractMessage
{
  /** Create an empty message. */
  public Warning() {
    this(null);
  }

  /**
   * Create a warning message with the given text.
   *
   * @param text the message text
   */
  public Warning(final String text) {
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
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isWarning() {
    return true;
  }
}
