import setuptools

with open("README.md", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name="MyNoSQL",
	version="0.0.6",
	author="damies13",
	author_email="damies13+mynosql@gmail.com",
	description="MyNoSQL",
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/damies13/mynosql",
	packages=setuptools.find_packages(exclude=["build/*"]),
	install_requires=['requests', 'HTTPServer'],
	classifiers=[
		"Development Status :: 3 - Alpha",
		"Topic :: Database :: Database Engines/Servers",
		"Programming Language :: Python :: 3.8",
		"License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
		"Operating System :: OS Independent",
	],
	# threading.get_native_id() needs python 3.8	https://docs.python.org/3/library/threading.html#threading.get_native_id
	python_requires='>=3.8',
	project_urls={
		'Getting Help': 'https://github.com/damies13/mynosql',
		'Source': 'https://github.com/damies13/mynosql',
	},
)
