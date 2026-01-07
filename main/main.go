package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// 基于 test_test.go 中的测试样例构建的演示程序
// 这个程序可以运行 raft 包中的测试函数

func main() {
	// 解析命令行参数
	testName := flag.String("test", "", "要运行的测试名称 (如: TestInitialElection2A, TestBasicAgree2B, TestReElection2A)")
	listTests := flag.Bool("list", false, "列出所有可用的测试")
	flag.Parse()

	if *listTests {
		listAvailableTests()
		return
	}

	if *testName == "" {
		fmt.Println("=== Raft 测试运行器 ===")
		fmt.Println("基于 raft/test_test.go 中的测试样例")
		fmt.Println()
		fmt.Println("使用方法:")
		fmt.Println("  go run main.go -test <测试名称>")
		fmt.Println("  go run main.go -list  # 列出所有可用测试")
		fmt.Println()
		fmt.Println("示例:")
		fmt.Println("  go run main.go -test TestInitialElection2A")
		fmt.Println("  go run main.go -test TestBasicAgree2B")
		fmt.Println("  go run main.go -test TestReElection2A")
		return
	}

	// 运行指定的测试
	runTest(*testName)
}

// 列出所有可用的测试
func listAvailableTests() {
	fmt.Println("=== 可用的 Raft 测试 ===")
	fmt.Println("\n2A 测试 (Leader 选举):")
	fmt.Println("  - TestInitialElection2A: 初始选举测试")
	fmt.Println("  - TestReElection2A: 重新选举测试")
	fmt.Println("  - TestManyElections2A: 多次选举测试")

	fmt.Println("\n2B 测试 (日志复制):")
	fmt.Println("  - TestBasicAgree2B: 基本一致性测试")
	fmt.Println("  - TestRPCBytes2B: RPC 字节数测试")
	fmt.Println("  - TestFollowerFailure2B: Follower 故障测试")
	fmt.Println("  - TestLeaderFailure2B: Leader 故障测试")
	fmt.Println("  - TestFailAgree2B: 故障后一致性测试")
	fmt.Println("  - TestFailNoAgree2B: 无多数派一致性测试")
	fmt.Println("  - TestConcurrentStarts2B: 并发 Start 测试")
	fmt.Println("  - TestRejoin2B: 重新加入测试")
	fmt.Println("  - TestBackup2B: Leader 备份测试")
	fmt.Println("  - TestCount2B: RPC 计数测试")

	fmt.Println("\n2C 测试 (持久化):")
	fmt.Println("  - TestPersist12C: 基本持久化测试")
	fmt.Println("  - TestPersist22C: 更多持久化测试")
	fmt.Println("  - TestPersist32C: 分区 Leader 崩溃测试")
	fmt.Println("  - TestFigure82C: Figure 8 场景测试")
	fmt.Println("  - TestUnreliableAgree2C: 不可靠网络一致性测试")
	fmt.Println("  - TestFigure8Unreliable2C: 不可靠网络 Figure 8 测试")
	fmt.Println("  - TestReliableChurn2C: 可靠网络扰动测试")
	fmt.Println("  - TestUnreliableChurn2C: 不可靠网络扰动测试")

	fmt.Println("\n2D 测试 (快照):")
	fmt.Println("  - TestSnapshotBasic2D: 基本快照测试")
	fmt.Println("  - TestSnapshotInstall2D: 快照安装测试")
	fmt.Println("  - TestSnapshotInstallUnreliable2D: 不可靠网络快照安装测试")
	fmt.Println("  - TestSnapshotInstallCrash2D: 崩溃快照安装测试")
	fmt.Println("  - TestSnapshotInstallUnCrash2D: 不可靠+崩溃快照测试")
	fmt.Println("  - TestSnapshotAllCrash2D: 所有节点崩溃测试")
	fmt.Println("  - TestSnapshotInit2D: 快照初始化测试")
}

// 运行指定的测试
func runTest(testName string) {
	fmt.Printf("运行测试: %s\n", testName)
	fmt.Println("=====================================")
	fmt.Println()

	// 获取项目根目录
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filename))
	raftDir := filepath.Join(projectRoot, "raft")

	// 构建 go test 命令
	cmd := exec.Command("go", "test", "-run", "^"+testName+"$", "-v")
	cmd.Dir = raftDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("在目录 %s 中运行测试...\n\n", raftDir)

	// 运行测试
	err := cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			fmt.Printf("\n✗ 测试 %s 失败 (退出码: %d)\n", testName, exitError.ExitCode())
			os.Exit(exitError.ExitCode())
		} else {
			fmt.Printf("\n✗ 运行测试时出错: %v\n", err)
			fmt.Printf("\n提示: 请确保已安装 Go 并且可以在 raft 目录中运行 'go test'\n")
			os.Exit(1)
		}
	} else {
		fmt.Printf("\n✓ 测试 %s 通过\n", testName)
	}
}
