package main

import (
	"encoding/json"
)

type MessageStructure struct {
	Message json.RawMessage `json:"message"`
	Header  string          `json:"header"`
	Key     string          `json:"key"`
}

type RequestStructure struct {
	Topic    string             `json:"topic"`
	Messages []MessageStructure `json:"messages"`
}
