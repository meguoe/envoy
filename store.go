package main

// store.go —— 数据持久化

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	xdsServer "envoy-control-plane/xds_server"
)

var storePath = func() string {
	if p := os.Getenv("XDS_STORE_PATH"); p != "" {
		return p
	}
	return "data/rules.json"
}()

func loadRules() ([]*xdsServer.ProxyRule, error) {
	data, err := os.ReadFile(storePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("📁 数据文件不存在，从空状态启动  path=%s", storePath)
			return nil, nil
		}
		return nil, fmt.Errorf("读取数据文件失败: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}

	var list []xdsServer.ProxyRule
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("解析数据文件失败: %w", err)
	}

	rules := make([]*xdsServer.ProxyRule, 0, len(list))
	for i := range list {
		if err := xdsServer.ValidateRule(&list[i]); err != nil {
			log.Printf("⚠️  跳过非法规则 #%d: %v", i, err)
			continue
		}
		rules = append(rules, &list[i])
	}
	log.Printf("📁 已从文件加载 %d 条规则  path=%s", len(rules), storePath)
	return rules, nil
}

func saveRules(list []*xdsServer.ProxyRule) error {
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	data, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	if err := atomicWrite(storePath, data); err != nil {
		return fmt.Errorf("写入失败: %w", err)
	}
	return nil
}

func atomicWrite(target string, data []byte) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*.json")
	if err != nil {
		return err
	}
	path := tmp.Name()
	defer os.Remove(path)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(path, target)
}
