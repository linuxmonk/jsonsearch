package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/TylerBrock/colorjson"
	"github.com/gusaki/jsonsearch/pkg/jsondb"
)

func clearScreenLinux() *exec.Cmd {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	return cmd
}

func clearScreenMacOS() *exec.Cmd {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	return cmd
}

func clearScreenWin() *exec.Cmd {
	cmd := exec.Command("cmd", "/c", "cls")
	cmd.Stdout = os.Stdout
	return cmd
}

func ClearScreen() {
	plat := runtime.GOOS
	switch plat {
	case "linux":
		clearScreenLinux().Run()
	case "windows":
		clearScreenWin().Run()
	case "darwin":
		clearScreenMacOS().Run()
	}
}

func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	str, _ := reader.ReadString('\n')
	return strings.TrimSpace(str)
}

func runInteractive(jsonDb *jsondb.JsonDB, relations KeyRelations) {
	for {
		ClearScreen()
		fmt.Println(">> Press CTRL-C to terminate the program <<")
		fmt.Print("Enter the JSON name to search: ")
		dbname := readLine()
		fmt.Print("Enter the name of the key to lookup: ")
		key := readLine()
		fmt.Print("Enter the value to lookup: ")
		value := readLine()
		results, err := jsonDb.Search(dbname, key, value, relations)
		if err != nil {
			fmt.Println(">>> ", err)
			fmt.Print("Press enter to continue...")
			readLine()
			continue
		}
		PrintResults(results)
		fmt.Print("Press enter to continue...")
		readLine()
	}
}

func PrintResults(results []interface{}) {
	if len(results) == 0 {
		fmt.Println("Data not found")
	}
	f := colorjson.NewFormatter()
	f.Indent = 4
	for _, r := range results {
		s, _ := f.Marshal(r)
		fmt.Println(string(s))
	}
}
