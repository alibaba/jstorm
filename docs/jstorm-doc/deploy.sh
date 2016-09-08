#!/usr/bin/env bash
HAS_JEKYLL=true

command -v jekyll > /dev/null
if [ $? -ne 0 ]; then
	echo -n "ERROR: Could not find jekyll. "
	echo "Please install with 'gem install jekyll' (see http://jekyllrb.com)."

	HAS_JEKYLL=false
	exit
fi

command -v redcarpet > /dev/null
if [ $? -ne 0 ]; then
	echo -n "WARN: Could not find redcarpet. "
	echo -n "Please install with 'sudo gem install redcarpet' (see https://github.com/vmg/redcarpet). "
	echo "Redcarpet is needed for Markdown parsing and table of contents generation."
fi

command -v pygmentize > /dev/null
if [ $? -ne 0 ]; then
	echo -n "WARN: Could not find pygments. "
	echo -n "Please install with 'sudo easy_install Pygments' (requires Python; see http://pygments.org). "
	echo "Pygments is needed for syntax highlighting of the code examples."
fi

echo "Start to build doc by Jekyll..."
jekyll build > /dev/null
echo "Build complete..."

echo "Setting up Git deployment..."
rm -rf .deploy_tmp
mkdir .deploy_tmp
cp -r _site/* .deploy_tmp/ > /dev/null
echo "Deploying..."
cd .deploy_tmp
rm -rf jekyll/
rm deploy.bat
git config --global http.postBuffer 8528000 > /dev/null
git init > /dev/null
git add -A > /dev/null
git commit -m "updated documents" > /dev/null
git push -u https://github.com/alibaba/jstorm.git HEAD:documents --force
cd ..
rm -rf .deploy_tmp
echo "Deploy successfully!"