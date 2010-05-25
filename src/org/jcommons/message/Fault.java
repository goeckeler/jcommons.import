package org.jcommons.message;

/**
 * An error message.
 */
public class Fault
  extends AbstractMessage
{
  /** Create an empty message. */
  public Fault() {
    this(null);
  }

  /**
   * Create an error message with the given text.
   *
   * @param text the message text
   */
  public Fault(final String text) {
    super();
    setText(text);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isError() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isInfo() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isWarning() {
    return false;
  }
}
