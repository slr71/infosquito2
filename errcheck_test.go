package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLogIfErr_CallsFunctionAndLogsError(t *testing.T) {
	var buf bytes.Buffer
	origOut := logrus.StandardLogger().Out
	logrus.SetOutput(&buf)
	defer logrus.SetOutput(origOut)

	called := false
	logIfErr(func() error {
		called = true
		return errors.New("boom")
	}, "closing widget")

	if !called {
		t.Fatal("logIfErr did not invoke the supplied function")
	}
	out := buf.String()
	if !strings.Contains(out, "closing widget") || !strings.Contains(out, "boom") {
		t.Fatalf("expected log to contain context and error, got: %q", out)
	}
}

func TestLogIfErr_NoLogOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	origOut := logrus.StandardLogger().Out
	logrus.SetOutput(&buf)
	defer logrus.SetOutput(origOut)

	logIfErr(func() error { return nil }, "closing widget")

	if buf.Len() != 0 {
		t.Fatalf("expected no log output on success, got: %q", buf.String())
	}
}
