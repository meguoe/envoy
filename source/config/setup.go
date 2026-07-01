package config

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// readEnvFile 读取 .env 文件并解析为键值对映射。
func readEnvFile(path string) map[string]string {
	env := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		return env
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexByte(line, '='); i >= 0 {
			env[line[:i]] = line[i+1:]
		}
	}
	return env
}

// RunSetupWizard 启动交互式配置向导，引导用户设置数据库连接和 API 密钥。
func RunSetupWizard(configPath string) error {
	reader := bufio.NewReader(os.Stdin)
	existing := readEnvFile(".env")

	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║         系统配置向导                 ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()

	fmt.Print("数据库主机地址 [" + getOrDefault(existing, "DB_HOST", "localhost") + "]: ")
	host, _ := reader.ReadString('\n')
	host = strings.TrimSpace(host)
	if host == "" {
		host = getOrDefault(existing, "DB_HOST", "localhost")
	}

	fmt.Print("数据库端口 [" + getOrDefault(existing, "DB_PORT", "5432") + "]: ")
	port, _ := reader.ReadString('\n')
	port = strings.TrimSpace(port)
	if port == "" {
		port = getOrDefault(existing, "DB_PORT", "5432")
	}

	fmt.Print("数据库用户名 [" + getOrDefault(existing, "DB_USER", "postgres") + "]: ")
	user, _ := reader.ReadString('\n')
	user = strings.TrimSpace(user)
	if user == "" {
		user = getOrDefault(existing, "DB_USER", "postgres")
	}

	if existing["DB_PASSWORD"] != "" {
		fmt.Print("数据库密码 [****]: ")
	} else {
		fmt.Print("数据库密码: ")
	}
	pass, _ := reader.ReadString('\n')
	pass = strings.TrimSpace(pass)

	fmt.Print("数据库名称 [" + getOrDefault(existing, "DB_NAME", "envoy_cp") + "]: ")
	dbname, _ := reader.ReadString('\n')
	dbname = strings.TrimSpace(dbname)
	if dbname == "" {
		dbname = getOrDefault(existing, "DB_NAME", "envoy_cp")
	}

	fmt.Println()
	fmt.Println("── API 认证──")
	if existing["API_KEY"] != "" {
		fmt.Println("当前 API 密钥: ****")
		fmt.Print("输入新密钥 [回车保留现有，输入 g 自动生成]: ")
	} else {
		fmt.Print("API 密钥 (输入 g 自动生成): ")
	}
	apiKeyInput, _ := reader.ReadString('\n')
	apiKeyInput = strings.TrimSpace(apiKeyInput)

	var apiKey string
	switch apiKeyInput {
	case "g", "G":
		apiKey = generateAPIKey()
		fmt.Printf("已生成 API 密钥: %s\n", apiKey)
	case "":
		if existing["API_KEY"] != "" {
			apiKey = existing["API_KEY"]
		}
	default:
		apiKey = apiKeyInput
	}

	fmt.Println()

	envMap := existing
	envMap["DB_HOST"] = host
	envMap["DB_PORT"] = port
	envMap["DB_USER"] = user
	if pass != "" {
		envMap["DB_PASSWORD"] = pass
	}
	envMap["DB_NAME"] = dbname
	if apiKey != "" {
		envMap["API_KEY"] = apiKey
	}
	var lines []string
	for k, v := range envMap {
		lines = append(lines, fmt.Sprintf("%s=%s", k, v))
	}
	envContent := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(".env", []byte(envContent), 0600); err != nil {
		return fmt.Errorf("写入环境变量失败: %w", err)
	}
	return nil
}

// getOrDefault 从映射中获取指定 key 的值，若不存在或为空则返回 fallback。
func getOrDefault(m map[string]string, key, fallback string) string {
	if v, ok := m[key]; ok && v != "" {
		return v
	}
	return fallback
}

// generateAPIKey 生成 32 字符的随机十六进制字符串作为 API 密钥。
func generateAPIKey() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("生成 API 密钥失败: %w", err))
	}
	return hex.EncodeToString(b)
}
