package common

import "errors"

var (
	ErrNotConnected      = errors.New("Not connected")
	ErrTimeOut           = errors.New("Timeout")
	ErrObjectNotFound    = errors.New("Object Not Found")
	ErrConnKilled        = errors.New("Connection Killed")
	ErrNotImplemented    = errors.New("Not Implemented")
	ErrInvalidPara       = errors.New("Invalid parameters")
	ErrUnrecognizedMagic = errors.New("Unrecognized magic number")
	ErrRead              = errors.New("Read fault")
	ErrVolumeExists      = errors.New("volume exists")
	ErrVolumeNotExist    = errors.New("volume not exist")
	ErrGetIndexFailed    = errors.New("Failed to retrive index")
	ErrBrickNotRegistered= errors.New("Brick not registered")
	ErrNoTaskLeft        = errors.New("No task left")
	ErrVolumeStarted     = errors.New("Volume started")
	ErrVolumeNotStarted     = errors.New("Volume not started")
)
