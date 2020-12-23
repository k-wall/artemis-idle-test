package io.artemis_idle_test;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.EmptyFrame;
import org.apache.qpid.proton.amqp.transport.FrameBody;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.engine.TransportDecodeException;
import org.apache.qpid.proton.engine.TransportException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class App
{

    private static int _localMaxFrameSize = 4096;
    private static boolean peerSentClosed;

    private enum State
    {
        HEADER0,
        HEADER1,
        HEADER2,
        HEADER3,
        HEADER4,
        HEADER5,
        HEADER6,
        HEADER7,
        SIZE_0,
        SIZE_1,
        SIZE_2,
        SIZE_3,
        PRE_PARSE,
        BUFFERING,
        PARSING,
        ERROR
    }


    public static final byte[] HEADER = "AMQP\0\1\0\0".getBytes();

    public static void main(String[] args ) throws Exception
    {
        int portNumber = 5672;

        boolean server = false;
        boolean answerClose = false;
        boolean shutdownSocket = false;
        List<String> strings = new ArrayList<>(Arrays.asList(args));
        Iterator<String> iterator = strings.iterator();
        while(iterator.hasNext()) {
            String arg = iterator.next();
            if (arg.equals("--server")) {
                server = true;
                iterator.remove();
            } else if (arg.equals("--answer-close")) {
                answerClose = true;
                iterator.remove();
            } else if (arg.equals("--shutdown-socket")) {
                shutdownSocket = true;
                iterator.remove();
            }
        }

        if (strings.size() > 0) {
            try {
                portNumber = Integer.parseInt(strings.get(strings.size() - 1));
            } catch (NumberFormatException e) {
            }
        }

        if (server) {
            final boolean finalAnswerClose = answerClose;
            final boolean finalShutdownSocket = shutdownSocket;

            System.out.println("Accepting on port " + portNumber);
            ServerSocket serverSocket = new ServerSocket(portNumber);
            while (true) {
                final Socket socket = serverSocket.accept();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            amqpOpenAndWait(socket, finalAnswerClose, finalShutdownSocket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }).start();
            }
        } else {
            final String host = "localhost";
            System.out.println("Creating socket to '" + host + "' on port " + portNumber);
            Socket socket = new Socket(host, portNumber);

            amqpOpenAndWait(socket, answerClose, shutdownSocket);

        }

    }

    private static void amqpOpenAndWait(Socket socket, boolean answerClose, boolean shutdownSocket) throws IOException, InterruptedException {

        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        byte[] buf = new byte[4096];
        boolean headerSent = false;
        boolean openSent = false;
        peerSentClosed = false;

        State state = State.HEADER0;

        while (true) {

            if (!headerSent) {
                outputStream.write(HEADER);
                headerSent = true;
                System.err.println("written header");
            }

            if (!openSent) {

                Open open = new Open();
                open.setContainerId("test");
                sendPerformative(outputStream, encoder, open);
                openSent = true;
                System.err.println("written open");

            }

            if (answerClose && peerSentClosed) {
                sendPerformative(outputStream, encoder, new Close());
                System.err.println("written close");
                if (shutdownSocket) {
                    socket.shutdownOutput();
                    System.err.println("shutdown socket");
                }
                break;
            } else if (shutdownSocket && peerSentClosed) {
                socket.shutdownOutput();
                System.err.println("shutdown socket only");
                break;
            }

            int read = inputStream.read(buf, 0, buf.length);

            if (read == -1) {
                System.err.printf("Peer closed socket %s %s%n", socket.isInputShutdown(), socket.isOutputShutdown());
                Thread.sleep(1000);
                continue;
            }
            ByteBuffer in = ByteBuffer.allocate(read);
            in.put(buf, 0, read);
            in.rewind();

            state = foo(in, state, decoder);


        }
    }

    private static void sendPerformative(OutputStream outputStream, EncoderImpl encoder, FrameBody frame) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.putInt(0); // SIZE placeholder
        b.put((byte)2); // DOFF
        b.put((byte)0); // TYPE
        b.putShort((short)0); // CHANNEL
        encoder.setByteBuffer(b);
        encoder.writeObject(frame);
        b.flip();
        int len = b.limit();
        b.putInt(len);
        outputStream.write(b.array(), 0, len);
    }

    private static State foo(ByteBuffer in, State state, DecoderImpl decoder) {

        ByteBuffer _frameBuffer = null;
        TransportException frameParsingError = null;

        int size = 0;
        ByteBuffer oldIn = null;
        while(in.hasRemaining() && state != State.ERROR)
        {
            switch(state)
            {
                case HEADER0:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[0])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[0], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER1;
                    }
                    else
                    {
                        break;
                    }
                case HEADER1:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[1])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[1], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER2;
                    }
                    else
                    {
                        break;
                    }
                case HEADER2:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[2])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[2], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER3;
                    }
                    else
                    {
                        break;
                    }
                case HEADER3:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[3])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[3], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER4;
                    }
                    else
                    {
                        break;
                    }
                case HEADER4:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[4])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[4], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER5;
                    }
                    else
                    {
                        break;
                    }
                case HEADER5:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[5])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[5], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER6;
                    }
                    else
                    {
                        break;
                    }
                case HEADER6:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[6])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[6], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER7;
                    }
                    else
                    {
                        break;
                    }
                case HEADER7:
                    if(in.hasRemaining())
                    {
                        byte c = in.get();
                        if(c != HEADER[7])
                        {
                            frameParsingError = new TransportException("AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[7], state);
                            state = State.ERROR;
                            break;
                        }

                        state = State.SIZE_0;
                        System.err.println("Read header");

                    }
                    else
                    {
                        break;
                    }
                case SIZE_0:
                    if(!in.hasRemaining())
                    {
                        break;
                    }
                    if(in.remaining() >= 4)
                    {
                        size = in.getInt();
                        state = State.PRE_PARSE;
                        break;
                    }
                    else
                    {
                        size = (in.get() << 24) & 0xFF000000;
                        if(!in.hasRemaining())
                        {
                            state = State.SIZE_1;
                            break;
                        }
                    }
                case SIZE_1:
                    size |= (in.get() << 16) & 0xFF0000;
                    if(!in.hasRemaining())
                    {
                        state = State.SIZE_2;
                        break;
                    }
                case SIZE_2:
                    size |= (in.get() << 8) & 0xFF00;
                    if(!in.hasRemaining())
                    {
                        state = State.SIZE_3;
                        break;
                    }
                case SIZE_3:
                    size |= in.get() & 0xFF;
                    state = State.PRE_PARSE;

                case PRE_PARSE:
                    if(size < 8)
                    {
                        frameParsingError = new TransportException("specified frame size %d smaller than minimum frame header "
                                + "size %d",
                                size, 8);
                        state = State.ERROR;
                        break;
                    }

                    if (_localMaxFrameSize > 0 && size > _localMaxFrameSize)
                    {
                        frameParsingError = new TransportException("specified frame size %d greater than maximum valid frame size %d",
                                size, _localMaxFrameSize);
                        state = State.ERROR;
                        break;
                    }

                    if(in.remaining() < size-4)
                    {
                        _frameBuffer = ByteBuffer.allocate(size-4);
                        _frameBuffer.put(in);
                        state = State.BUFFERING;
                        break;
                    }
                case BUFFERING:
                    if(_frameBuffer != null)
                    {
                        if(in.remaining() < _frameBuffer.remaining())
                        {
                            _frameBuffer.put(in);
                            break;
                        }
                        else
                        {
                            ByteBuffer dup = in.duplicate();
                            dup.limit(dup.position()+_frameBuffer.remaining());
                            in.position(in.position()+_frameBuffer.remaining());
                            _frameBuffer.put(dup);
                            oldIn = in;
                            _frameBuffer.flip();
                            in = _frameBuffer;
                            state = State.PARSING;
                        }
                    }

                case PARSING:

                    int dataOffset = (in.get() << 2) & 0x3FF;

                    if(dataOffset < 8)
                    {
                        frameParsingError = new TransportException("specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8);
                        state = State.ERROR;
                        break;
                    }
                    else if(dataOffset > size)
                    {
                        frameParsingError = new TransportException("specified frame data offset %d larger than the frame size %d", dataOffset, size);
                        state = State.ERROR;
                        break;
                    }

                    // type

                    int type = in.get() & 0xFF;
                    int channel = in.getShort() & 0xFFFF;

                    if(type != 0)
                    {
                        frameParsingError = new TransportException("unknown frame type: %d", type);
                        state = State.ERROR;
                        break;
                    }

                    // note that this skips over the extended header if it's present
                    if(dataOffset!=8)
                    {
                        in.position(in.position()+dataOffset-8);
                    }

                    // oldIn null iff not working on duplicated buffer
                    final int frameBodySize = size - dataOffset;
                    if(oldIn == null)
                    {
                        oldIn = in;
                        in = in.duplicate();
                        final int endPos = in.position() + frameBodySize;
                        in.limit(endPos);
                        oldIn.position(endPos);

                    }

                    try
                    {

                        Binary payload = null;
                        Object val = null;

                        if (frameBodySize > 0)
                        {
                            decoder.setByteBuffer(in);
                            val = decoder.readObject();
                            decoder.setByteBuffer(null);

                            if(in.hasRemaining())
                            {
                                byte[] payloadBytes = new byte[in.remaining()];
                                in.get(payloadBytes);
                                payload = new Binary(payloadBytes);
                            }
                            else
                            {
                                payload = null;
                            }
                        }
                        else
                        {
                            val = EmptyFrame.INSTANCE;
                        }

                        if(val instanceof FrameBody)
                        {
                            FrameBody frameBody = (FrameBody) val;
                            System.err.println("IN: CH["+channel+"] : " + frameBody + (payload == null ? "" : "[" + payload + "]"));

                            if (frameBody instanceof Close) {
                                peerSentClosed = true;
                            }

                        }
                        else
                        {
                            throw new TransportException("Frameparser encountered a "
                                    + (val == null? "null" : val.getClass())
                                    + " which is not a " + FrameBody.class);
                        }

                        size = 0;
                        state = State.SIZE_0;
                        in = oldIn;
                        oldIn = null;
                        _frameBuffer = null;
                        state = State.SIZE_0;
                    }
                    catch (DecodeException ex)
                    {
                        state = State.ERROR;
                        String message = ex.getMessage() == null ? ex.toString(): ex.getMessage();

                        frameParsingError = new TransportDecodeException(message, ex);
                    }
                    break;
                case ERROR:
                    // do nothing
            }

        }

        if (frameParsingError != null) {
            throw frameParsingError;
        }

        return state;
    }

}
