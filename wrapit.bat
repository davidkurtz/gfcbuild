set ORACLE_HOME=C:\app\client\david.kurtz\product\12.1.0\client_1
%ORACLE_HOME%\BIN\wrap iname=gfcbuildpkgbody.sql 
pause

rem copy gfcbuildpkgbody.plb C:\Users\david\customers\abbey\gfcbuild\
rem copy gfcbuildpkgbody.plb C:\Users\david\customers\hays\gfcbuild\
rem copy gfcbuildpkgbody.plb C:\Users\david\customers\morrisons\gfcbuild\
copy gfcbuildpkgbody.plb u:\customers\prologis\gfcbuild\

rem copy gfcbuildpkg.sql C:\Users\david\customers\abbey\gfcbuild\
rem copy gfcbuildpkg.sql C:\Users\david\customers\hays\gfcbuild\
rem copy gfcbuildpkg.sql C:\Users\david\customers\morrisons\gfcbuild\

rem copy gfcbuildpkgbody.plb e:\
rem copy gfcbuildpkgbody.sql e:\
rem copy gfcbuildpkg.sql e:\
rem copy gfcbuildtab.sql e:\
rem copy gfcbuildprivs.sql e:\
pause
