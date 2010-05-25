package org.jcommons.message;

import java.util.Collections;
import java.util.List;

import org.jcommons.lang.string.StringUtils;

/**
 * Abstraction of a concrete message so that sub-classes only need to indicate their type.
 *
 * @author Thorsten Goeckeler
 */
public abstract class AbstractMessage
  implements Message
{
  private String text;

  /**
   * Set the error text for this message.
   *
   * @param text the text that forms this message, can be <code>null</code>, but that should be avoided
   * @return this to allow chaining
   */
  protected final AbstractMessage setText(final String text) {
    this.text = text;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public final Message add(final Message text) {
    // simply replace the current text
    this.setText(text.getText());
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public final Message clear() {
    // clear the current text
    setText(null);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public final String getFaults() {
    if (isError()) {
      return getText();
    }
    return StringUtils.EMPTY;
  }

  /** {@inheritDoc} */
  @Override
  public final String getInfos() {
    if (isInfo()) {
      return getText();
    }
    return StringUtils.EMPTY;
  }

  /** {@inheritDoc} */
  @Override
  public final String getText() {
    return StringUtils.defaultString(text);
  }

  /** {@inheritDoc} */
  @Override
  public final List<? extends Message> getTexts() {
    if (isEmpty()) return Collections.emptyList();
    return Collections.singletonList(this);
  }

  /** {@inheritDoc} */
  @Override
  public final List<? extends Message> getMessages() {
    return getTexts();
  }

  /** {@inheritDoc} */
  @Override
  public final String getWarnings() {
    if (isWarning()) {
      return getText();
    }
    return StringUtils.EMPTY;
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isEmpty() {
    return StringUtils.isBlank(getText());
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isComposite() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public abstract boolean isError();

  /** {@inheritDoc} */
  @Override
  public abstract boolean isInfo();

  /** {@inheritDoc} */
  @Override
  public abstract boolean isWarning();

  /** {@inheritDoc} */
  @Override
  public final Message remove(final Message text) {
    // well, what shall be removed here?
    return clear();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getText();
  }
}
