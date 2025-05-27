package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const rqlitedBinaryPath = "/tmp/rqlited" // Path to the built rqlited binary

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestJoinExistingSingleNodeClusterLeader(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for Raft data
	dataDir := t.TempDir()

	// --- First run: Initialize a single-node cluster ---
	httpPort1, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free HTTP port for run 1: %v", err)
	}
	raftPort1, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free Raft port for run 1: %v", err)
	}

	argsRun1 := []string{
		"-node-id", "node0",
		"-http-addr", fmt.Sprintf("localhost:%d", httpPort1),
		"-raft-addr", fmt.Sprintf("localhost:%d", raftPort1),
		dataDir,
	}
	cmdRun1 := exec.Command(rqlitedBinaryPath, argsRun1...)
	cmdRun1.Stderr = os.Stderr // For debugging the first run if needed
	cmdRun1.Stdout = os.Stdout

	t.Logf("Starting rqlited (run 1) with args: %v", strings.Join(argsRun1, " "))
	if err := cmdRun1.Start(); err != nil {
		t.Fatalf("failed to start rqlited (run 1): %v", err)
	}

	// Wait for the node to initialize.
	// A more robust check would be to poll the HTTP API's /status endpoint.
	time.Sleep(3 * time.Second)

	// Send SIGINT to gracefully shut down the node
	t.Logf("Sending SIGINT to rqlited (run 1) process %d", cmdRun1.Process.Pid)
	if err := cmdRun1.Process.Signal(syscall.SIGINT); err != nil {
		// If the process already exited, it might be fine or it might be a problem.
		// We'll let cmd.Wait() determine if it was a clean exit.
		t.Logf("failed to send SIGINT to rqlited (run 1), process might have already exited: %v", err)
	}

	// Wait for the first instance to shut down
	errRun1 := cmdRun1.Wait()
	if errRun1 != nil {
		// An ExitError is expected if it received SIGINT.
		// We are more concerned if it *didn't* exit cleanly or exited too early.
		if exitErr, ok := errRun1.(*exec.ExitError); ok {
			// SIGINT typically results in a non-zero exit status.
			// On Unix, exit status is 130 for SIGINT (128 + 2).
			// On Windows, it might be different.
			// We'll log it but not fail the test here, as the main point is the second run.
			t.Logf("rqlited (run 1) exited with status: %s. stderr: %s", exitErr, string(exitErr.Stderr))
		} else {
			t.Fatalf("rqlited (run 1) wait failed with unexpected error: %v", errRun1)
		}
	} else {
		t.Logf("rqlited (run 1) exited cleanly.")
	}
	t.Logf("rqlited (run 1) shut down.")

	// --- Second run: Attempt to join an existing cluster ---
	httpPort2, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free HTTP port for run 2: %v", err)
	}
	raftPort2, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free Raft port for run 2: %v", err)
	}

	// Use a dummy join address. It shouldn't even try to connect.
	joinAddr := "localhost:12345"
	argsRun2 := []string{
		"-node-id", "node0", // Same node ID
		"-http-addr", fmt.Sprintf("localhost:%d", httpPort2),
		"-raft-addr", fmt.Sprintf("localhost:%d", raftPort2),
		"-join", joinAddr,
		dataDir, // Same data directory
	}

	cmdRun2 := exec.Command(rqlitedBinaryPath, argsRun2...)
	var stderrBuffer bytes.Buffer
	cmdRun2.Stderr = &stderrBuffer

	t.Logf("Starting rqlited (run 2) with args: %v", strings.Join(argsRun2, " "))
	errRun2 := cmdRun2.Run() // Use Run() as we expect it to exit

	// Verification
	if errRun2 == nil {
		t.Fatalf("rqlited (run 2) was expected to exit with an error, but it ran successfully.")
	}

	exitErr, ok := errRun2.(*exec.ExitError)
	if !ok {
		t.Fatalf("rqlited (run 2) failed with an unexpected error type: %v", errRun2)
	}

	if exitErr.ExitCode() == 0 {
		t.Errorf("rqlited (run 2) exited with code 0, expected non-zero. Stderr:\n%s", stderrBuffer.String())
	}

	expectedErrorMsg := "node is already a single-node cluster leader, joining a cluster is not permitted"
	if !strings.Contains(stderrBuffer.String(), expectedErrorMsg) {
		t.Errorf("rqlited (run 2) stderr did not contain the expected message.\nExpected: %s\nGot:\n%s", expectedErrorMsg, stderrBuffer.String())
	} else {
		t.Logf("rqlited (run 2) exited with expected error: %s", stderrBuffer.String())
	}
}
