#!/usr/bin/env python3
"""
yt-dlp 自動補丁腳本
功能：註解 targetDurationSec 條件和緊隨的 continue 語句
用法：python patch_script.py
"""

import os
import sys
import re

def get_target_file_path():
    """獲取目標檔案路徑"""
    try:
        import yt_dlp
        # yt_dlp.__file__ 示例: /usr/local/lib/python3.13/site-packages/yt_dlp/__init__.py
        ytdlp_dir = os.path.dirname(yt_dlp.__file__)
        # 正確路徑: ytdlp_dir/extractor/youtube/_video.py
        target_file = os.path.join(ytdlp_dir, 'extractor', 'youtube', '_video.py')
        return target_file
    except ImportError:
        # 備用方法
        import subprocess
        try:
            result = subprocess.run(['pip', 'show', 'yt-dlp'], 
                                  capture_output=True, text=True)
            for line in result.stdout.split('\n'):
                if line.startswith('Location:'):
                    base_path = line.split(': ')[1].strip()
                    target_file = os.path.join(base_path, 'yt_dlp', 'extractor', 'youtube', '_video.py')
                    return target_file
        except:
            pass
    return None

def apply_patch():
    """應用補丁"""
    print("開始應用 yt-dlp 補丁...")
    
    # 獲取目標檔案路徑
    target_file = get_target_file_path()
    if not target_file:
        print("❌ 錯誤: 無法找到 yt-dlp 安裝路徑")
        return False
    
    print(f"目標檔案: {target_file}")
    
    # 檢查檔案是否存在
    if not os.path.exists(target_file):
        print(f"❌ 錯誤: 找不到目標檔案")
        return False
    
    try:
        # 讀取檔案內容
        with open(target_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"檔案讀取成功，共 {len(lines)} 行")
        
        # 尋找 targetDurationSec 行
        target_line_index = -1
        for i, line in enumerate(lines):
            if 'targetDurationSec' in line and not line.strip().startswith('#'):
                target_line_index = i
                print(f"✅ 在第 {i+1} 行找到 targetDurationSec")
                break
        
        if target_line_index == -1:
            # 檢查是否已經被註解
            for i, line in enumerate(lines):
                if 'targetDurationSec' in line and line.strip().startswith('#'):
                    print(f"✅ targetDurationSec 已經在第 {i+1} 行被註解")
                    return True
            
            print("❌ 錯誤: 未找到 targetDurationSec")
            return False
        
        # 尋找緊隨的 continue 行
        continue_line_index = -1
        for i in range(target_line_index + 1, min(target_line_index + 5, len(lines))):
            if 'continue' in lines[i] and not lines[i].strip().startswith('#'):
                continue_line_index = i
                print(f"✅ 在第 {i+1} 行找到 continue")
                break
        
        if continue_line_index == -1:
            print("❌ 錯誤: 在目標行附近找不到 continue")
            return False
        
        # 顯示修改前的內容
        print("\n🔍 修改前的內容:")
        start = max(0, target_line_index - 1)
        end = min(len(lines), continue_line_index + 2)
        for i in range(start, end):
            prefix = '>>> ' if i in [target_line_index, continue_line_index] else '    '
            print(f"{prefix}{i+1:4d}: {lines[i].rstrip()}")
        
        # 計算縮進並註解
        target_indent = len(lines[target_line_index]) - len(lines[target_line_index].lstrip())
        continue_indent = len(lines[continue_line_index]) - len(lines[continue_line_index].lstrip())
        
        lines[target_line_index] = ' ' * target_indent + '# ' + lines[target_line_index].lstrip()
        lines[continue_line_index] = ' ' * continue_indent + '# ' + lines[continue_line_index].lstrip()
        
        # 寫回檔案
        with open(target_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        # 顯示修改後的內容
        print("\n✅ 修改後的內容:")
        for i in range(start, end):
            prefix = '>>> ' if i in [target_line_index, continue_line_index] else '    '
            print(f"{prefix}{i+1:4d}: {lines[i].rstrip()}")
        
        print(f"\n🎉 補丁成功應用！修改了第 {target_line_index+1} 行和第 {continue_line_index+1} 行")
        return True
        
    except Exception as e:
        print(f"❌ 補丁應用失敗: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_patch():
    """驗證補丁"""
    print("開始驗證補丁...")
    
    target_file = get_target_file_path()
    if not target_file:
        print("❌ 無法獲取目標檔案路徑")
        return False
    
    print(f"檢查檔案: {target_file}")
    
    if not os.path.exists(target_file):
        print("❌ 檔案不存在")
        return False
    
    try:
        with open(target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 檢查是否已註解
        lines = content.split('\n')
        patch_found = False
        
        for i, line in enumerate(lines):
            if 'targetDurationSec' in line:
                print(f"\n找到 targetDurationSec 在第 {i+1} 行:")
                print(f"  內容: {line.strip()}")
                
                if line.strip().startswith('#'):
                    print("  ✅ 已註解")
                    
                    # 檢查下一行是否為已註解的 continue
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        if 'continue' in next_line and next_line.strip().startswith('#'):
                            print(f"  下一行 ({i+2}): {next_line.strip()}")
                            print("  ✅ continue 也已註解")
                            patch_found = True
                        else:
                            print(f"  下一行 ({i+2}): {next_line.strip()}")
                            print("  ⚠️  不是已註解的 continue")
                    else:
                        print("  ⚠️  沒有下一行")
                else:
                    print("  ❌ 未註解")
                
                break
        
        if patch_found:
            print("\n✅ 補丁驗證成功！")
            return True
        else:
            print("\n❌ 補丁未正確應用")
            return False
            
    except Exception as e:
        print(f"❌ 驗證失敗: {e}")
        return False

def main():
    """主函數"""
    print("=" * 60)
    print("yt-dlp 自動補丁腳本")
    print("=" * 60)
    
    # 檢查命令行參數
    mode = 'auto'
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    
    if mode == 'patch':
        # 只應用補丁
        success = apply_patch()
    elif mode == 'verify':
        # 只驗證補丁
        success = verify_patch()
    elif mode == 'auto' or mode == '':
        # 自動模式：先驗證，如果未修補則修補
        print("🔍 檢查是否已修補...")
        if verify_patch():
            success = True
        else:
            print("\n" + "=" * 50)
            print("未檢測到有效補丁，開始應用補丁...")
            print("=" * 50)
            success = apply_patch()
    else:
        print(f"❌ 未知模式: {mode}")
        print("可用模式: patch, verify, auto")
        success = False
    
    print("=" * 60)
    if success:
        print("✅ 操作成功完成！")
        return True
    else:
        print("❌ 操作失敗")
        return False

if __name__ == '__main__':
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  操作被中斷")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 發生未預期錯誤: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
