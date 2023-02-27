package main

import (
	"encoding/json"
)

type MessageStructure struct {
	Message json.RawMessage `json:"message"`
	Header  string          `json:"header"`
}

type RequestStructure struct {
	Topic    string             `json:"topic"`
	Messages []MessageStructure `json:"messages"`
}