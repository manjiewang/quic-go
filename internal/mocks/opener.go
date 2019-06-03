// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/lucas-clemente/quic-go/internal/handshake (interfaces: Opener)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	protocol "github.com/lucas-clemente/quic-go/internal/protocol"
)

// MockOpener is a mock of Opener interface
type MockOpener struct {
	ctrl     *gomock.Controller
	recorder *MockOpenerMockRecorder
}

// MockOpenerMockRecorder is the mock recorder for MockOpener
type MockOpenerMockRecorder struct {
	mock *MockOpener
}

// NewMockOpener creates a new mock instance
func NewMockOpener(ctrl *gomock.Controller) *MockOpener {
	mock := &MockOpener{ctrl: ctrl}
	mock.recorder = &MockOpenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockOpener) EXPECT() *MockOpenerMockRecorder {
	return m.recorder
}

// DecryptHeader mocks base method
func (m *MockOpener) DecryptHeader(arg0 []byte, arg1 *byte, arg2 []byte) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DecryptHeader", arg0, arg1, arg2)
}

// DecryptHeader indicates an expected call of DecryptHeader
func (mr *MockOpenerMockRecorder) DecryptHeader(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DecryptHeader", reflect.TypeOf((*MockOpener)(nil).DecryptHeader), arg0, arg1, arg2)
}

// Open mocks base method
func (m *MockOpener) Open(arg0, arg1 []byte, arg2 protocol.PacketNumber, arg3 protocol.KeyPhase, arg4 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Open indicates an expected call of Open
func (mr *MockOpenerMockRecorder) Open(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockOpener)(nil).Open), arg0, arg1, arg2, arg3, arg4)
}
