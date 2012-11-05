#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
# Copyright (c) 2010, 2011 PostgreSQL Global Development Group
#
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://wiki.tada.se/index.php?title=PLJava_License
#
# @author Thomas Hallgren
#
# Top level Makefile for PLJava
#
# To compile a PLJava for PostgreSQL 8.x the makefile system will utilize
# the PostgreSQL pgxs system. The only prerequisite for such a compile is
# that a PostgreSQL 8.x is installed on the system and that the PATH is set
# so that the binaries of this installed can be executed.
#
# The following options are recognized (aside from normal options like
# CFLAGS etc.)
#
#   PGSQLDIR=<pgsql root>  For old style (not pgxs based) compilation
#
#   USE_GCJ=1              Builds a shared object file containing both
#                          C and Java code. Requires GCJ 3.4 or later.
#
#   PGXS                   To explicitly set the PXGS file.
#
#   PLJAVA_LDFLAGS         Added to the final link command.
#
#   USE_LD_RPATH=<n>       Adds run time link path to the final object
#                          file.  Using this requires knowledge of your
#                          linker.
#
#                          Accepted values for <n> are
#
#                          1 Adds -Wl,-rpath,<jvm-library>
#                          2 Adds -rpath <jvm-library>
#                          3 Adds -R<jvm-library>
#                          4 Adds -R <jvm-library>
#                          5 Adds -Wl,-R<jvm-library>
#                          6 Adds -Wl,-R,<jvm-library>
#
#-------------------------------------------------------------------------

# Can't use $(shell pwd) directly since /bin/pwd doesn't handle mounted
# volumes very well on some systems. The /mnt/hgfs that enables host shares
# in a vmware client is one example of this.
#
export PROJDIR := $(shell $(SHELL) -c pwd)

export PG_CONFIG ?= pg_config
export PGXS ?= $(shell $(PG_CONFIG) --pgxs)
export PGSQLSRC := $(dir $(PGXS))..
export top_builddir := $(PGSQLSRC)/..

export TARGETDIR		:= $(PROJDIR)/build
export OBJDIR			:= $(TARGETDIR)/objs
export JNIDIR			:= $(TARGETDIR)/jni
export CLASSDIR			:= $(TARGETDIR)/classes
export PLJAVA_MAJOR_VER	:= 2
export PLJAVA_MINOR_VER	:= 0
export PLJAVA_PATCH_VER	:= 0
export PLJAVA_VERSION	:= $(PLJAVA_MAJOR_VER).$(PLJAVA_MINOR_VER).$(PLJAVA_PATCH_VER)
export TAR				:= /bin/tar

OS := $(shell uname -s)
MACHINE := $(shell uname -m)

.PHONY: all clean docs javadoc source_tarball maven_bundle install uninstall depend release \
	c_all c_install c_uninstall c_depend \
	pljava_all pljava_javadoc \
	deploy_all deploy_javadoc \
	examples_all examples_javadoc \
	test_all test_javadoc

all: pljava_all deploy_all c_all examples_all

install: c_install

uninstall: c_uninstall

depend: c_depend

docs:
	@-mkdir -p $(TARGETDIR)
	@find docs \( \
		   -name CVS \
		-o -name .cvsignore \
		\) -prune -o \( -type f -print \) | cpio -pmud "$(TARGETDIR)"
	@cp COPYRIGHT $(TARGETDIR)/docs/COPYRIGHT.txt

javadoc: pljava_javadoc deploy_javadoc examples_javadoc

clean:
	@-rm -rf $(TARGETDIR)

pljava_all pljava_javadoc: pljava_%:
	@-mkdir -p $(CLASSDIR)/pljava
	@$(MAKE) -r -C $(CLASSDIR)/pljava -f $(PROJDIR)/src/java/pljava/Makefile \
	MODULEROOT=$(PROJDIR)/src/java $*

deploy_all deploy_javadoc: deploy_%:
	@-mkdir -p $(CLASSDIR)/deploy
	@$(MAKE) -r -C $(CLASSDIR)/deploy -f $(PROJDIR)/src/java/deploy/Makefile \
	MODULEROOT=$(PROJDIR)/src/java $*

examples_all: examples_%: pljava_all
	@-mkdir -p $(CLASSDIR)/examples
	@$(MAKE) -r -C $(CLASSDIR)/examples -f $(PROJDIR)/src/java/examples/Makefile \
	MODULEROOT=$(PROJDIR)/src/java $*

test_all: test_%:
	@-mkdir -p $(CLASSDIR)/test
	@$(MAKE) -r -C $(CLASSDIR)/test -f $(PROJDIR)/src/java/test/Makefile \
	MODULEROOT=$(PROJDIR)/src/java $*

c_all c_install c_uninstall c_depend: c_%:
	@-mkdir -p $(OBJDIR)
	@$(MAKE) -r -C $(OBJDIR) -f $(PROJDIR)/src/C/pljava/Makefile \
	MODULEROOT=$(PROJDIR)/src/C build_$*

source_tarball:
	@-mkdir -p $(TARGETDIR)/distrib
	@$(MAKE) -r -C $(TARGETDIR) -f $(PROJDIR)/packaging/Makefile $@

release: all docs javadoc
	@-mkdir -p $(TARGETDIR)/distrib
	@$(MAKE) -r -C $(TARGETDIR) -f $(PROJDIR)/packaging/Makefile $@

maven_bundle: pljava_all
	@-mkdir -p $(TARGETDIR)/distrib
	@$(MAKE) -r -C $(TARGETDIR) -f $(PROJDIR)/packaging/Makefile $@
