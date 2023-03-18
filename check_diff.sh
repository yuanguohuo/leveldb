#!/bin/bash

sed -i -e 's#^+[[:space:]]*//.*##' diff
sed -i -e 's#^-[[:space:]]*//.*##' diff
