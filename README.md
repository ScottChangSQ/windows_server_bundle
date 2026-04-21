# Windows 服务器部署包

这个目录就是要复制到服务器上的唯一部署目录，不需要再上传整个仓库。

## 目录说明

- `mt5_gateway/`
  - MT5 网关主程序、轻量管理面板、静态页面、依赖清单、环境示例、EA 文件
- `windows/`
  - Windows 部署脚本、Caddy 反向代理配置、自启脚本

## 推荐上传路径

```text
C:\mt5_bundle\windows_server_bundle
```

上传后目录应类似：

```text
C:\mt5_bundle
└─ windows_server_bundle
   ├─ mt5_gateway
   ├─ windows
   ├─ deploy_bundle.cmd
   └─ deploy_bundle.ps1
```

## 一键部署

```powershell
双击 deploy_bundle.cmd
```

或：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\mt5_bundle\windows_server_bundle\deploy_bundle.ps1 -Mode Gui -BundleRoot "C:\mt5_bundle\windows_server_bundle"
```

它会自动完成：

- 停掉旧计划任务、旧网关、旧管理面板、旧 Caddy / Nginx
- 强制释放 `80 / 443 / 2019 / 8787 / 8788`
- 检查脚本语法
- 补齐 Python 依赖
- 重新注册网关与管理面板计划任务
- 隐藏启动 Caddy
- 自动验收 `8787 / 8788 / 80 / 443(loopback SNI) / 443(public) / /admin/`

服务器端只会弹出一个状态窗口，用来显示步骤成功与否、当前状态和日志。这个窗口可以关闭，关闭后不会影响后台服务继续运行。

## caddy.exe 位置说明

部署包默认不内置 `caddy.exe`，你可以把它放在下列任一位置：

```text
C:\mt5_bundle\windows_server_bundle\windows\caddy.exe
```

或：

```text
C:\mt5_bundle\windows_server_bundle\caddy.exe
```

或（兼容历史位置）：

```text
C:\mt5_bundle\caddy.exe
```

## 重要安全边界

远程账号会话 `/v2/session/*` 必须通过 HTTPS 公网入口开放；默认 HTTP 的 `Caddyfile` 会直接拒绝这些接口。

## 手动启动检查

```powershell
cd C:\mt5_bundle\windows_server_bundle\mt5_gateway
.\start_gateway.ps1
.\start_admin_panel.ps1
```

```powershell
cd C:\mt5_bundle\windows_server_bundle
.\windows\03_run_healthcheck.ps1
Invoke-WebRequest http://127.0.0.1/admin/ -UseBasicParsing
```
