import type { Frame, FrameHeaders, StompVersion } from '../codec/frame.ts';

export type ClientCommand =
  | 'CONNECT'
  | 'STOMP'
  | 'SEND'
  | 'SUBSCRIBE'
  | 'UNSUBSCRIBE'
  | 'ACK'
  | 'NACK'
  | 'BEGIN'
  | 'COMMIT'
  | 'ABORT'
  | 'DISCONNECT';

export type ServerCommand = 'CONNECTED' | 'MESSAGE' | 'RECEIPT' | 'ERROR';

export type Command = ClientCommand | ServerCommand;

export type AckMode = 'auto' | 'client' | 'client-individual';

// ---------------------------------------------------------------------------
// Headers
//
// Header types are intersections with `FrameHeaders`, so that typed frames
// remain assignable to plain frames and may carry arbitrary extra headers.
// Header types whose shape differs between versions take the negotiated
// version as a type parameter; with the default (all versions) they resolve
// to the union of every version's shape.
// ---------------------------------------------------------------------------

export type ConnectHeaders = FrameHeaders & {
  readonly 'accept-version'?: string;
  readonly host?: string;
  readonly login?: string;
  readonly passcode?: string;
  readonly 'heart-beat'?: string;
};

export type StompConnectHeaders = ConnectHeaders & {
  readonly 'accept-version': string;
};

export type SendHeaders = FrameHeaders & {
  readonly destination: string;
  readonly transaction?: string;
  readonly receipt?: string;
  readonly 'content-type'?: string;
  readonly 'content-length'?: string;
};

export type SubscribeHeaders<V extends StompVersion = StompVersion> = FrameHeaders & {
  readonly destination: string;
  readonly ack?: AckMode;
  readonly receipt?: string;
} & (V extends '1.0' ? { readonly id?: string } : { readonly id: string });

export type UnsubscribeHeaders<V extends StompVersion = StompVersion> = FrameHeaders & {
  readonly receipt?: string;
} & (V extends '1.0' ? { readonly id?: string; readonly destination?: string } : { readonly id: string });

export type AckHeaders<V extends StompVersion = StompVersion> = FrameHeaders & {
  readonly transaction?: string;
  readonly receipt?: string;
} & (V extends '1.2'
    ? { readonly id: string }
    : V extends '1.1'
      ? { readonly 'message-id': string; readonly subscription: string }
      : { readonly 'message-id': string });

export type TransactionHeaders = FrameHeaders & {
  readonly transaction: string;
  readonly receipt?: string;
};

export type DisconnectHeaders = FrameHeaders & {
  readonly receipt?: string;
};

export type ConnectedHeaders = FrameHeaders & {
  readonly version?: string;
  readonly session?: string;
  readonly server?: string;
  readonly 'heart-beat'?: string;
};

export type MessageHeaders<V extends StompVersion = StompVersion> = FrameHeaders & {
  readonly destination: string;
  readonly 'message-id': string;
  readonly ack?: string;
  readonly 'content-type'?: string;
  readonly 'content-length'?: string;
} & (V extends '1.0' ? { readonly subscription?: string } : { readonly subscription: string });

export type ReceiptHeaders = FrameHeaders & {
  readonly 'receipt-id': string;
};

export type ErrorHeaders = FrameHeaders & {
  readonly message?: string;
  readonly 'receipt-id'?: string;
  readonly 'content-type'?: string;
  readonly 'content-length'?: string;
};

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

export type ConnectFrame = Frame<'CONNECT', ConnectHeaders>;
export type StompConnectFrame = Frame<'STOMP', StompConnectHeaders>;
export type SendFrame = Frame<'SEND', SendHeaders>;
export type SubscribeFrame<V extends StompVersion = StompVersion> = Frame<'SUBSCRIBE', SubscribeHeaders<V>>;
export type UnsubscribeFrame<V extends StompVersion = StompVersion> = Frame<'UNSUBSCRIBE', UnsubscribeHeaders<V>>;
export type AckFrame<V extends StompVersion = StompVersion> = Frame<'ACK', AckHeaders<V>>;
/** `NACK` does not exist in STOMP 1.0. */
export type NackFrame<V extends StompVersion = StompVersion> = V extends '1.0' ? never : Frame<'NACK', AckHeaders<V>>;
export type BeginFrame = Frame<'BEGIN', TransactionHeaders>;
export type CommitFrame = Frame<'COMMIT', TransactionHeaders>;
export type AbortFrame = Frame<'ABORT', TransactionHeaders>;
export type DisconnectFrame = Frame<'DISCONNECT', DisconnectHeaders>;

export type ConnectedFrame = Frame<'CONNECTED', ConnectedHeaders>;
export type MessageFrame<V extends StompVersion = StompVersion> = Frame<'MESSAGE', MessageHeaders<V>>;
export type ReceiptFrame = Frame<'RECEIPT', ReceiptHeaders>;
export type ErrorFrame = Frame<'ERROR', ErrorHeaders>;

/** Frames sent by clients, for a given negotiated version. */
export type ClientFrame<V extends StompVersion = StompVersion> =
  | ConnectFrame
  | StompConnectFrame
  | SendFrame
  | SubscribeFrame<V>
  | UnsubscribeFrame<V>
  | AckFrame<V>
  | NackFrame<V>
  | BeginFrame
  | CommitFrame
  | AbortFrame
  | DisconnectFrame;

/** Frames sent by servers, for a given negotiated version. */
export type ServerFrame<V extends StompVersion = StompVersion> =
  | ConnectedFrame
  | MessageFrame<V>
  | ReceiptFrame
  | ErrorFrame;
