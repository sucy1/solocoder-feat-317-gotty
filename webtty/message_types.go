package webtty

// Protocols defines the name of this protocol,
// which is supposed to be used to the subprotocol of Websockt streams.
var Protocols = []string{"webtty"}

const (
	UnknownInput = '0'
	Input = '1'
	Ping = '2'
	ResizeTerminal = '3'
	PlayControl = '4'
)

const (
	UnknownOutput = '0'
	Output = '1'
	Pong = '2'
	SetWindowTitle = '3'
	SetPreferences = '4'
	SetReconnect = '5'
	SetPlayMode = '6'
)
