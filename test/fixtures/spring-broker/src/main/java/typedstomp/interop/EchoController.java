package typedstomp.interop;

import java.util.Locale;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

/** Application destinations, under the {@code /app} prefix. */
@Controller
public class EchoController {

  /** Sends {@code /app/echo} payloads back on {@code /topic/echo}. */
  @MessageMapping("/echo")
  @SendTo("/topic/echo")
  public String echo(String payload) {
    return payload;
  }

  /** Sends {@code /app/upper} payloads, upper-cased, to {@code /topic/upper}. */
  @MessageMapping("/upper")
  @SendTo("/topic/upper")
  public String upper(String payload) {
    return payload.toUpperCase(Locale.ROOT);
  }
}
