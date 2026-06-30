package store

// store.go —— 数据持久化

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	xdsserver "envoy-control-plane/internal/server/xds"
)

func Load(path string) ([]*xdsserver.ProxyRule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("数据文件不存在，从空状态启动  path=%s", path)
			return nil, nil
		}
		return nil, fmt.Errorf("读取数据文件失败: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}

	var list []xdsserver.ProxyRule
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("解析数据文件失败: %w", err)
	}

	rules := make([]*xdsserver.ProxyRule, 0, len(list))
	seenIDs := make(map[string]struct{}, len(list))
	for i := range list {
		if list[i].ID == "" {
			log.Printf("跳过非法规则 #%d: id 不能为空", i)
			continue
		}
		if _, ok := seenIDs[list[i].ID]; ok {
			log.Printf("跳过非法规则 #%d: id %q 重复", i, list[i].ID)
			continue
		}
		if err := xdsserver.ValidateRule(&list[i]); err != nil {
			log.Printf("跳过非法规则 #%d: %v", i, err)
			continue
		}
		xdsserver.NormalizeRule(&list[i])
		seenIDs[list[i].ID] = struct{}{}
		rules = append(rules, &list[i])
	}

	// 与 Save 保持一致：按 ID 排序
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})

	log.Printf("已从文件加载 %d 条规则  path=%s", len(rules), path)
	return rules, nil
}

func Save(path string, list []*xdsserver.ProxyRule) error {
	sorted := make([]*xdsserver.ProxyRule, len(list))
	copy(sorted, list)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	list = sorted

	data, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	if err := atomicWrite(path, data); err != nil {
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
