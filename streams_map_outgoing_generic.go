package quic

import (
	"sync"

	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/wire"
)

//go:generate genny -in $GOFILE -out streams_map_outgoing_bidi.go gen "item=streamI Item=BidiStream streamTypeGeneric=protocol.StreamTypeBidi"
//go:generate genny -in $GOFILE -out streams_map_outgoing_uni.go gen "item=sendStreamI Item=UniStream streamTypeGeneric=protocol.StreamTypeUni"
type outgoingItemsMap struct {
	mutex sync.RWMutex

	openQueue          map[protocol.StreamNum]chan struct{}
	lowestInOpenQueue  protocol.StreamNum // if the map is empty, this value has no meaning
	highestInOpenQueue protocol.StreamNum // if the map is empty, this value has no meaning

	streams map[protocol.StreamNum]item

	nextStream  protocol.StreamNum // stream ID of the stream returned by OpenStream(Sync)
	maxStream   protocol.StreamNum // the maximum stream ID we're allowed to open
	blockedSent bool               // was a STREAMS_BLOCKED sent for the current maxStream

	newStream            func(protocol.StreamNum) item
	queueStreamIDBlocked func(*wire.StreamsBlockedFrame)

	closeErr error
}

func newOutgoingItemsMap(
	newStream func(protocol.StreamNum) item,
	queueControlFrame func(wire.Frame),
) *outgoingItemsMap {
	return &outgoingItemsMap{
		streams:              make(map[protocol.StreamNum]item),
		openQueue:            make(map[protocol.StreamNum]chan struct{}),
		maxStream:            protocol.InvalidStreamNum,
		nextStream:           1,
		newStream:            newStream,
		queueStreamIDBlocked: func(f *wire.StreamsBlockedFrame) { queueControlFrame(f) },
	}
}

func (m *outgoingItemsMap) OpenStream() (item, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.closeErr != nil {
		return nil, m.closeErr
	}

	str, err := m.openStreamImpl()
	if err != nil {
		return nil, streamOpenErr{err}
	}
	return str, nil
}

func (m *outgoingItemsMap) OpenStreamSync() (item, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.closeErr != nil {
		return nil, m.closeErr
	}
	str, err := m.openStreamImpl()
	if err == nil {
		return str, nil
	}
	if err != errTooManyOpenStreams {
		return nil, streamOpenErr{err}
	}

	waitChan := make(chan struct{})
	if len(m.openQueue) == 0 {
		m.lowestInOpenQueue = m.nextStream
		m.highestInOpenQueue = m.nextStream
	} else {
		m.highestInOpenQueue++
	}
	m.openQueue[m.highestInOpenQueue] = waitChan

	m.mutex.Unlock()

	<-waitChan

	m.mutex.Lock()
	if m.closeErr != nil {
		return nil, m.closeErr
	}
	str, err = m.openStreamImpl()
	if err == nil {
		return str, nil
	}
	// should never happen
	return nil, streamOpenErr{err}
}

func (m *outgoingItemsMap) openStreamImpl() (item, error) {
	if m.nextStream > m.maxStream {
		if !m.blockedSent {
			var streamNum protocol.StreamNum
			if m.maxStream != protocol.InvalidStreamNum {
				streamNum = m.maxStream
			}
			m.queueStreamIDBlocked(&wire.StreamsBlockedFrame{
				Type:        streamTypeGeneric,
				StreamLimit: streamNum,
			})
			m.blockedSent = true
		}
		return nil, errTooManyOpenStreams
	}
	s := m.newStream(m.nextStream)
	m.streams[m.nextStream] = s
	m.nextStream++
	return s, nil
}

func (m *outgoingItemsMap) GetStream(num protocol.StreamNum) (item, error) {
	m.mutex.RLock()
	if num >= m.nextStream {
		m.mutex.RUnlock()
		return nil, streamError{
			message: "peer attempted to open stream %d",
			nums:    []protocol.StreamNum{num},
		}
	}
	s := m.streams[num]
	m.mutex.RUnlock()
	return s, nil
}

func (m *outgoingItemsMap) DeleteStream(num protocol.StreamNum) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.streams[num]; !ok {
		return streamError{
			message: "Tried to delete unknown stream %d",
			nums:    []protocol.StreamNum{num},
		}
	}
	delete(m.streams, num)
	return nil
}

func (m *outgoingItemsMap) SetMaxStream(num protocol.StreamNum) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if num <= m.maxStream {
		return
	}
	m.maxStream = num
	m.blockedSent = false
	if len(m.openQueue) > 0 {
		maxNum := num
		if maxNum > m.highestInOpenQueue {
			maxNum = m.highestInOpenQueue
		}
		for n := m.lowestInOpenQueue; n <= maxNum; n++ {
			close(m.openQueue[n])
			delete(m.openQueue, n)
		}
		m.lowestInOpenQueue = maxNum + 1
	}
}

func (m *outgoingItemsMap) CloseWithError(err error) {
	m.mutex.Lock()
	m.closeErr = err
	for _, str := range m.streams {
		str.closeForShutdown(err)
	}
	for _, c := range m.openQueue {
		close(c)
	}
	m.mutex.Unlock()
}
