package typedstomp.interop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring fixture for typed-stomp interoperability tests.
 *
 * <ul>
 *   <li>By default, runs a STOMP over WebSocket broker on port 8080 (endpoint {@code /ws}),
 *       see {@link BrokerConfig} and {@link EchoController}.</li>
 *   <li>With {@code MODE=client}, runs Spring's STOMP client against the server at
 *       {@code TARGET_URL} instead, see {@link InteropClient}, and exits with 0 on success.</li>
 * </ul>
 */
@SpringBootApplication
public class InteropApplication {

  public static void main(String[] args) throws Exception {
    if ("client".equals(System.getenv("MODE"))) {
      System.exit(InteropClient.run(System.getenv("TARGET_URL")));
    }
    SpringApplication.run(InteropApplication.class, args);
  }
}
