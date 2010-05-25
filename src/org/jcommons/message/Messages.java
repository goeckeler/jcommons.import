package org.jcommons.message;

import static org.jcommons.message.Flatten.flatten;
import static org.jcommons.message.ToText.toText;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.jcommons.functional.Functions;
import org.jcommons.functional.function.UnaryFunction;
import org.jcommons.functional.predicate.UnaryPredicate;

/**
 * A composite message that collects many message in one go and acts as the default message container.
 *
 * @author Thorsten Goeckeler
 */
public class Messages
  implements Message
{
  private final List<Message> messages = new ArrayList<Message>();

  /** {@inheritDoc} */
  @Override
  public final Message add(final Message text) {
    if (text != null) messages.add(text);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public final Message clear() {
    messages.clear();
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public final String getFaults() {
    // filter on errors only
    List<Message> errors = Functions.filter(Predicates.concreteErrorsOnly(), flatten(this));
    // map errors to their strings
    return StringUtils.join(Functions.map(toText(), errors), " ");
  }

  /** {@inheritDoc} */
  @Override
  public final String getInfos() {
    // filter on infos only
    List<Message> infos = Functions.filter(Predicates.concreteInfosOnly(), flatten(this));
    // map infos to their strings
    return StringUtils.join(Functions.map(toText(), infos), " ");
  }

  /** {@inheritDoc} */
  @Override
  public final String getText() {
    // map all messages to their strings
    return StringUtils.join(Functions.map(toText(), messages), " ");
  }

  /** {@inheritDoc} */
  @Override
  public final List< ? extends Message> getTexts() {
    return flatten(this);
  }

  /** {@inheritDoc} */
  @Override
  public final List< ? extends Message> getMessages() {
    return Collections.unmodifiableList(messages);
  }

  /** {@inheritDoc} */
  @Override
  public final String getWarnings() {
    // filter on warnings only
    List<Message> warnings = Functions.filter(Predicates.concreteWarningsOnly(), flatten(this));
    // map warnings to their strings
    return StringUtils.join(Functions.map(toText(), warnings), " ");
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isEmpty() {
    return messages.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isComposite() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isError() {
    return Functions.some(Predicates.errorsOnly(), messages);
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isInfo() {
    return Functions.some(Predicates.infosOnly(), messages);
  }

  /** {@inheritDoc} */
  @Override
  public final boolean isWarning() {
    return Functions.some(Predicates.warningsOnly(), messages);
  }

  /** {@inheritDoc} */
  @Override
  public final Message remove(final Message text) {
    if (text != null) messages.remove(text);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return getText();
  }
}

/**
 * Retrieves the text for a given message.
 *
 * @author Thorsten Goeckeler
 */
class ToText
  implements UnaryFunction<String, Message>
{
  private static final ToText INSTANCE = new ToText();

  /** @return a singleton instance to avoid memory overhead */
  public static ToText toText() {
    return INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public String execute(final Message argument) {
    return argument.getText();
  }
}

/**
 * Flatten a message tree so that only concrete messages remain on the top level list.
 *
 * @author Thorsten Goeckeler
 */
final class Flatten
{
  /** hide sole constructor */
  private Flatten() {
  }

  /**
   * Flat out the message tree.
   *
   * @param message the current message to flat out
   * @return a list containing only concrete messages
   */
  public static List<Message> flatten(final Message message) {
    if (!message.isComposite()) return Collections.singletonList(message);

    List< ? extends Message> all = message.getMessages();
    List<Message> messages = new ArrayList<Message>(Math.max(10, all.size()));

    for (Message current : all) {
      messages.addAll(flatten(current));
    }

    return messages;
  }
}

/**
 * Filter messages.
 *
 * @author Thorsten Goeckeler
 */
abstract class MessageFilter
  implements UnaryPredicate<Message>
{
  private boolean includeComposites;

  /** filter on concrete messages only */
  public MessageFilter() {
    includeComposites = false;
  }

  /** @return true if composites shall be included, false otherwise */
  public final boolean includeComposites() {
    return includeComposites;
  }

  /**
   * Define whether composites are included or not.
   *
   * @param include true to include composites, otherwise false
   * @return this to allow chaining
   */
  public MessageFilter includeComposites(final boolean include) {
    this.includeComposites = include;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute(final Message argument) {
    if (argument.isComposite() && !includeComposites()) return false;
    return filter(argument);
  }

  /**
   * Indicates whether to include or exclude a message based on its type.
   *
   * @param message the current message
   * @return true if the message shall be included, false to exclude it
   */
  protected abstract boolean filter(final Message message);
}

/**
 * Filter on errors only.
 *
 * @author Thorsten Goeckeler
 */
class ErrorFilter
  extends MessageFilter
{
  /** {@inheritDoc} */
  @Override
  protected boolean filter(final Message message) {
    return message.isError();
  }
}

/**
 * Filter on warnings only.
 *
 * @author Thorsten Goeckeler
 */
class WarningFilter
  extends MessageFilter
{
  /** {@inheritDoc} */
  @Override
  protected boolean filter(final Message message) {
    return message.isWarning();
  }
}

/**
 * Filter on infos only.
 *
 * @author Thorsten Goeckeler
 */
class InfoFilter
  extends MessageFilter
{
  /** {@inheritDoc} */
  @Override
  protected boolean filter(final Message message) {
    return message.isInfo();
  }
}

/**
 * Factory to minimize memory overhead on predicate filters.
 *
 * @author Thorsten Goeckeler
 */
final class Predicates
{
  private static final MessageFilter CONCRETE_ERRORS = new ErrorFilter().includeComposites(false);
  private static final MessageFilter COMPOSITE_ERRORS = new ErrorFilter().includeComposites(true);
  private static final MessageFilter CONCRETE_WARNINGS = new WarningFilter().includeComposites(false);
  private static final MessageFilter COMPOSITE_WARNINGS = new WarningFilter().includeComposites(true);
  private static final MessageFilter CONCRETE_INFOS = new InfoFilter().includeComposites(false);
  private static final MessageFilter COMPOSITE_INFOS = new InfoFilter().includeComposites(true);

  /** hide sole constructor */
  private Predicates() {
  }

  /** @return filter on concrete errors */
  public static MessageFilter concreteErrorsOnly() {
    return CONCRETE_ERRORS;
  }

  /** @return filter on errors */
  public static MessageFilter errorsOnly() {
    return COMPOSITE_ERRORS;
  }

  /** @return filter on concrete warnings */
  public static MessageFilter concreteWarningsOnly() {
    return CONCRETE_WARNINGS;
  }

  /** @return filter on warnings */
  public static MessageFilter warningsOnly() {
    return COMPOSITE_WARNINGS;
  }

  /** @return filter on concrete infos */
  public static MessageFilter concreteInfosOnly() {
    return CONCRETE_INFOS;
  }

  /** @return filter on infos */
  public static MessageFilter infosOnly() {
    return COMPOSITE_INFOS;
  }
}
