import os
import shutil
import zipfile


def zip_dir(src_dir, output_file):
    with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as bundle:
        for root, dirs, files in os.walk(src_dir):
            dirs[:] = [d for d in dirs if d != '__pycache__']
            rel_root = os.path.relpath(root, src_dir)
            for filename in files:
                if filename.endswith('.pyc'):
                    continue
                full_path = os.path.join(root, filename)
                arcname = filename if rel_root == '.' else os.path.join(rel_root, filename)
                print(f'打包文件: {full_path} -> {arcname}')
                bundle.write(full_path, arcname)


if __name__ == '__main__':
    src_dir = 'src'
    out_dir = 'out'
    output_path = os.path.join(out_dir, 'Calibre-XSH.zip')

    if os.path.exists(out_dir):
        print(f'清理旧目录: {out_dir}')
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    zip_dir(src_dir, output_path)
    print(f'插件已输出到: {output_path}')
