PyFiware
=======================

A simple library to interact with the Fiware Orion Context broker. This library uses the v2 especification.
A sample project that exists as an aid to the `Python Packaging User Guide

`Fiware Orion project page
<https://github.com/telefonicaid/fiware-orion>`_.


----

To deploy a dev Fiware orion context broker you can use the docker stack file contained in the extra directory. As this:

    docker stack deploy -c extra/fiware-orion.yaml dev_fiware

This is CB is used in the integration test done in the test directory.