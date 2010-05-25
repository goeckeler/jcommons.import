package org.jcommons.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

/** Checks internal messaging system. */
public class MessagesTest
{
  private final Messages messages = new Messages();
  private final Fault fault = new Fault("fault.");
  private final Fault otherError = new Fault("other fault.");
  private final Warning warning = new Warning("warning.");
  private final Info info = new Info("info.");

  /** check concrete implementation */
  @Test
  public void testConcreteError() {
    assertEquals("fault.", fault.getText());
    assertEquals("fault.", fault.toString());
    assertEquals(otherError.getText(), fault.add(otherError).getText());
    assertTrue(fault.isError());
    assertTrue(warning.isWarning());
    assertTrue(info.isInfo());

    assertTrue(fault.clear().isEmpty());
    assertFalse(fault.isComposite());
    assertTrue(fault.add(otherError).remove(info).isEmpty());

    assertEquals("other fault.", otherError.getFaults());
    assertEquals("", otherError.getWarnings());
    assertEquals("", otherError.getInfos());

    assertEquals("warning.", warning.getWarnings());
    assertEquals("", warning.getFaults());
    assertEquals("", warning.getInfos());

    assertEquals("info.", info.getInfos());
    assertEquals("", info.getWarnings());
    assertEquals("", info.getFaults());

    assertSame(info, info.getTexts().iterator().next());
    assertSame(info, info.getMessages().iterator().next());
    assertTrue(fault.getTexts().isEmpty());

    assertNotNull(new Fault().getText());
    assertNotNull(new Warning().getText());
    assertNotNull(new Info().getText());
  }

  /** Check if generic functions are correctly implemented. */
  @Test
  public void testMessages() {
    assertTrue(messages.clear().isEmpty());
    messages.add(warning).add(fault).add(info).add(otherError);
    assertFalse(messages.isEmpty());
    assertTrue(messages.clear().isEmpty());

    messages.add(warning).add(fault).add(info).add(otherError);
    assertTrue(messages.isComposite());
    assertTrue(messages.isError());
    assertTrue(messages.isWarning());
    assertTrue(messages.isInfo());

    assertEquals("fault. other fault.", messages.getFaults());
    assertEquals("warning.", messages.getWarnings());
    assertEquals("info.", messages.getInfos());

    messages.remove(otherError);
    assertEquals("fault.", messages.getFaults());
    messages.remove(fault);
    assertEquals("", messages.getFaults());
    assertFalse(messages.isError());
    assertTrue(messages.isWarning());

    messages.clear().add(warning).add(fault).add(info).add(otherError);
    assertEquals("warning. fault. info. other fault.", messages.getText());
    assertEquals("warning. fault. info. other fault.", messages.toString());
    assertNotNull(messages.getTexts());
    assertEquals(4, messages.getTexts().size());
  }

  /** check that composites can contain composites */
  @Test
  public void testComposite() {
    Message errors = new Messages().add(fault).add(otherError);
    Message warnings = new Messages().add(warning);

    messages.add(errors).add(info).add(warnings);
    assertEquals("fault. other fault. info. warning.", messages.getText());
    assertEquals("fault. other fault.", messages.getFaults());
    assertEquals("warning.", messages.getWarnings());
    assertEquals("info.", messages.getInfos());

    Collection<? extends Message> list = messages.getTexts();
    assertNotNull(list);
    assertEquals(4, list.size());
  }
}
