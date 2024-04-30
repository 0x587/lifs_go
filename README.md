# LIFS_GO

## 文件结构 & 命名规范

### 接口模块

指一系列具有统一规范但有多种实现的模块

其目录结构以及文件名必须按照以下要求，以`kv`为例
```
kv
├── error.go                // 公用的错误（可选）
├── interfaces.go           // 公用的接口，命名为IF
├── file                    // 其中一种实现，单开一个子包，不得包含模块前缀（即不可为kvfile）
│   ├── file.go             // 具体实现，必须与子包同名，必须提供一个返回IF的New方法，实现命名为Impl
│   │                       // New方法只返回IF，错误直接panic 
│   │                       // 由于已有New方法，不需另外var _ kv.IF = (*Impl)(nil)检查类型
│   ├── xxxx1.go            // 实现内部细节
│   ├── xxxx2.go
│   └── file_test.go        // 实现单元测试
└── mem
    ├── mem.go
    └── mem_test.go

```

目前包含

- fs
  - mem
  - file
- kv
  - fuse
  - ftp
