"""SessionMap 内部模块：不对外暴露的实现细节。

本包包含 sessionmap 模块的内部实现，包括：
    - reconcile.py: 启动对账工具（从 ACP 侧获取会话 ID 列表）
    - session_caps.py: 会话能力缓存工具（模型/代理信息提取与渲染）
    - session_restore.py: 既有 session 的 resume/load 兼容恢复入口
    - storage.py: JSON 存储工具（读取/写入 session_map.json）

设计原则：
    - 内部模块（_ 前缀）：不对外暴露，仅被 sessionmap 包内模块使用
    - 工具函数：无状态的纯函数，方便测试
    - 兼容性：支持多种字段命名风格（snake_case / camelCase）
"""
