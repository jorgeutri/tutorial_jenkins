Se trata de un processor de Nifi, el cuál dado una cadena String detecta el idioma de dicha cadena.

El grado de precisión de detección del idioma se puede editar cambiando el atributo alpha(con valores entre 0-1),
este processor puede detectar varios idiomas en la cadena y muestra aquel idioma con una mayor probabilidad.

Para su funcionamiento incluimos la dependencia:

<dependency>
    <groupId>com.optimaize.languagedetector</groupId>
    <artifactId>language-detector</artifactId>
    <version>0.6</version>
</dependency>