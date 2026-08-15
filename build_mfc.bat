@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"

echo [1/3] 结束旧的共享进程...
taskkill /F /IM LanShareMfc.exe >nul 2>&1
taskkill /F /IM http_server.exe >nul 2>&1

echo [2/3] 查找 Visual Studio C++ 工具链...
set "VSWHERE=C:\PROGRA~2\Microsoft Visual Studio\Installer\vswhere.exe"
if exist "%VSWHERE%" goto vswhere_ready
echo 未找到 vswhere.exe，请安装 Visual Studio Installer。
pause
exit /b 1
:vswhere_ready
"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath > "%TEMP%\LanShareVsPath.txt"
set /p VSROOT=<"%TEMP%\LanShareVsPath.txt"
if defined VSROOT goto vs_ready
echo 未找到 C++ 桌面开发工具或 MFC 组件。
echo 请在 Visual Studio Installer 中安装：
echo   使用 C++ 的桌面开发
echo   C++ MFC for latest v142 build tools
pause
exit /b 1
:vs_ready

call "%VSROOT%\Common7\Tools\VsDevCmd.bat" -arch=x64 -host_arch=x64 >nul
if not errorlevel 1 goto devcmd_ready
echo 初始化 Visual Studio 编译环境失败。
pause
exit /b 1
:devcmd_ready

echo [3/3] 编译 MFC 版本...
msbuild "%~dp0LanShareMfc.vcxproj" /t:Rebuild /p:Configuration=Release /p:Platform=x64 /m
if not errorlevel 1 goto build_ok
echo 编译失败。
pause
exit /b 1
:build_ok

echo 编译成功：%~dp0bin\x64\Release\LanShareMfc.exe
pause
exit /b 0
