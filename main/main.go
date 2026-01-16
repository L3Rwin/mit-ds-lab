package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// Demo runner for tests defined in raft/test_test.go.

func main() {
	// Parse CLI flags.
	testName := flag.String("test", "", "Test name to run (e.g., TestInitialElection2A, TestBasicAgree2B, TestReElection2A)")
	listTests := flag.Bool("list", false, "List all available tests")
	flag.Parse()

	if *listTests {
		listAvailableTests()
		return
	}

	if *testName == "" {
		fmt.Println("=== Raft Test Runner ===")
		fmt.Println("Based on tests defined in raft/test_test.go")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  go run main.go -test <test name>")
		fmt.Println("  go run main.go -list  # list all available tests")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run main.go -test TestInitialElection2A")
		fmt.Println("  go run main.go -test TestBasicAgree2B")
		fmt.Println("  go run main.go -test TestReElection2A")
		return
	}

	// Run the selected test.
	runTest(*testName)
}

// List all supported tests.
func listAvailableTests() {
	fmt.Println("=== Available Raft Tests ===")
	fmt.Println("\n2A Tests (Leader Election):")
	fmt.Println("  - TestInitialElection2A: initial election")
	fmt.Println("  - TestReElection2A: re-election")
	fmt.Println("  - TestManyElections2A: many elections")

	fmt.Println("\n2B Tests (Log Replication):")
	fmt.Println("  - TestBasicAgree2B: basic agreement")
	fmt.Println("  - TestRPCBytes2B: RPC byte count")
	fmt.Println("  - TestFollowerFailure2B: follower failure")
	fmt.Println("  - TestLeaderFailure2B: leader failure")
	fmt.Println("  - TestFailAgree2B: agreement after failure")
	fmt.Println("  - TestFailNoAgree2B: no agreement without majority")
	fmt.Println("  - TestConcurrentStarts2B: concurrent Start")
	fmt.Println("  - TestRejoin2B: rejoin")
	fmt.Println("  - TestBackup2B: leader backup")
	fmt.Println("  - TestCount2B: RPC count")

	fmt.Println("\n2C Tests (Persistence):")
	fmt.Println("  - TestPersist12C: basic persistence")
	fmt.Println("  - TestPersist22C: more persistence")
	fmt.Println("  - TestPersist32C: partitioned leader crash")
	fmt.Println("  - TestFigure82C: Figure 8 scenario")
	fmt.Println("  - TestUnreliableAgree2C: agreement with unreliable network")
	fmt.Println("  - TestFigure8Unreliable2C: Figure 8 with unreliable network")
	fmt.Println("  - TestReliableChurn2C: reliable churn")
	fmt.Println("  - TestUnreliableChurn2C: unreliable churn")

	fmt.Println("\n2D Tests (Snapshots):")
	fmt.Println("  - TestSnapshotBasic2D: basic snapshot")
	fmt.Println("  - TestSnapshotInstall2D: snapshot install")
	fmt.Println("  - TestSnapshotInstallUnreliable2D: snapshot install with unreliable network")
	fmt.Println("  - TestSnapshotInstallCrash2D: snapshot install with crashes")
	fmt.Println("  - TestSnapshotInstallUnCrash2D: snapshot install with unreliable network + crashes")
	fmt.Println("  - TestSnapshotAllCrash2D: all nodes crash")
	fmt.Println("  - TestSnapshotInit2D: snapshot initialization")
}

// Run a specific test by name.
func runTest(testName string) {
	fmt.Printf("Running test: %s\n", testName)
	fmt.Println("=====================================")
	fmt.Println()

	// Resolve project root.
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filename))
	raftDir := filepath.Join(projectRoot, "raft")

	// Build go test command.
	cmd := exec.Command("go", "test", "-run", "^"+testName+"$", "-v")
	cmd.Dir = raftDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("Running test in %s...\n\n", raftDir)

	// Execute test.
	err := cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			fmt.Printf("\n✗ Test %s failed (exit code: %d)\n", testName, exitError.ExitCode())
			os.Exit(exitError.ExitCode())
		} else {
			fmt.Printf("\n✗ Error running test: %v\n", err)
			fmt.Printf("\nTip: ensure Go is installed and 'go test' works in the raft directory.\n")
			os.Exit(1)
		}
	} else {
		fmt.Printf("\n✓ Test %s passed\n", testName)
	}
}
