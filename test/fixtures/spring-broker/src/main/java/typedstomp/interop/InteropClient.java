package typedstomp.interop;

import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/**
 * Exercises a typed-stomp server with Spring's STOMP client:
 *
 * <ol>
 *   <li>connects (with heart-beats) to {@code TARGET_URL};</li>
 *   <li>subscribes to {@code /topic/to-spring};</li>
 *   <li>sends {@code ping} to {@code /topic/to-node}, with a receipt;</li>
 *   <li>expects {@code pong} on {@code /topic/to-spring}, sent by the test's Node client in reply;</li>
 *   <li>disconnects.</li>
 * </ol>
 *
 * Prints {@code INTEROP OK} and returns 0 on success.
 */
final class InteropClient {

  private InteropClient() {}

  static int run(String url) throws Exception {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.initialize();
    WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
    client.setMessageConverter(new StringMessageConverter());
    client.setTaskScheduler(scheduler);
    client.setDefaultHeartbeat(new long[] {1000, 1000});

    CompletableFuture<String> reply = new CompletableFuture<>();
    StompSessionHandlerAdapter handler =
        new StompSessionHandlerAdapter() {
          @Override
          public void handleException(
              StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
            reply.completeExceptionally(exception);
          }

          @Override
          public void handleTransportError(StompSession session, Throwable exception) {
            reply.completeExceptionally(exception);
          }
        };

    try {
      StompSession session = client.connectAsync(url, handler).get(10, TimeUnit.SECONDS);
      System.out.println("CONNECTED " + session.getSessionId());

      session.subscribe(
          "/topic/to-spring",
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return String.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              reply.complete((String) payload);
            }
          });

      session.setAutoReceipt(true);
      CompletableFuture<Void> receipt = new CompletableFuture<>();
      StompHeaders headers = new StompHeaders();
      headers.setDestination("/topic/to-node");
      session.send(headers, "ping").addReceiptTask(() -> receipt.complete(null));
      receipt.get(10, TimeUnit.SECONDS);
      System.out.println("RECEIPT received");

      String received = reply.get(10, TimeUnit.SECONDS);
      System.out.println("RECEIVED " + received);
      session.disconnect();

      if (!"pong".equals(received)) {
        System.out.println("INTEROP FAILED: unexpected reply " + received);
        return 1;
      }
      System.out.println("INTEROP OK");
      return 0;
    } catch (Exception e) {
      System.out.println("INTEROP FAILED: " + e);
      e.printStackTrace(System.out);
      return 1;
    } finally {
      client.stop();
      scheduler.shutdown();
    }
  }
}
