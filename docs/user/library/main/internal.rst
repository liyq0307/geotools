Internals
---------

The ``gt-main`` class holds a number of internal and abstract classes useful to those implementing support for additional ``DataStore`` formats.
These classes should be moved to ``gt-main`` when time permits.

The ``gt-main`` module provides many well known classes covered in the public documentation:

* ``DataAccessFinder``
* ``DataStoreFinder``
* ``FileDataStoreFinder``
* ``DataUtilities``
* ``DefaultRepository``
* ``DefaultTransaction``
* ``DefaultView``

A few public exceptions:

* ``FeatureLockException``
* ``SchemaNotFoundException``


AbstractDataStore
^^^^^^^^^^^^^^^^^

The ``AbstractDataStore`` is an old base class for ``DataStore`` implementations, currently used only by ``MemoryDataStore`` and ``PropertyDataStore`` (Old). All new stores use ``ContentDataStore`` instead.

.. image:: /images/AbstractDataStore.PNG

This class is helpful starting point, however we have taken the lessons learned and wrapped them up in ``ContentDataStore`` covered in the :doc:`gt-main <../data/internal>` documentation.

The following classes are related:

* ``AbstractDataStore``
* ``AbstractDataStoreFactory``
* ``AbstractDataStoreTest``
* ``AbstractFeatureSource``
* ``AbstractFeatureStore``
* ``AbstractFeatureLocking``
* ``DefaultFeatureResults`` - the original name of ``FeatureCollection`` was ``FeatureResults``
  
DataStore
^^^^^^^^^

There are a large number of DataStore helper classes in ``gt-main``.

* Default implementations:
  
  * ``DefaultResourceInfo``
  * ``DefaultServiceInfo``

* ``FileDataStore``
  
  * ``AbstractFileDataStore``

* Conformance test case:
  
  * ``DataTestCase``

* Managers
  
  These managers, with their supporting readers and writers are responsible for ``AbstractDataStore`` being able
  wrap transaction and locking support around you work.
  
  * ``FeatureListenerManager``
  * ``InProcessLockingManager``

* Transaction Implementation
  
  You can store a ``TransactionStateDiff`` in a Transaction, the ``DiffFeatureReader`` and ``DiffFeatureWriters`` will collect any changes for you
  in memory which you can review when the user calls commit.

  * ``TransactionStateDiff``
  * ``Diff``
  * ``DiffFeatureReader``
  * ``DiffFeatureWriter``

* Projection File
  
  * ``PrjFileReader``
  * ``WorldFileReader``
  * ``WorldFileWriter``

``FeatureCollection``
'''''''''''''''''''''

Lots of abstract and utility classes are available when working with ``FeatureCollection``:

* ``AbstractFeatureCollection``
* ``AbstractFeatureVisitor``
* ``AdaptorFeatureCollection``
* ``DecoratingFeatureCollection``
* ``DecoratingSimpleFeatureCollection``
* ``DelegateFeatureIterator``
* ``DelegateSimpleFeatureIterator``
* ``FilteringSimpleFeatureCollection``
* ``MaxSimpleFeatureCollection``
* ``SubFeatureCollection`` / ``FilteredIterator``
* ``SubFeatureList``

``FeatureCollections`` often form a pair with the iterator they use to make contents available:
* ``DataFeatureCollection`` / ``FeatureWriterIterator`` / ``FeatureReaderIterator``
* ``EmptyFeatureCollection`` / ``EmptyIterator``
* ``FilteringFeatureCollection`` / ``FilteringFeatureIterator``
* ``MaxFeaturesFeatureCollection`` / ``MaxFeaturesIterator``
* ``ReprojectingFeatureCollection`` / ``ReprojectingIterator``
* ``ReTypingFeatureCollection`` / ``ReTypingIterator``


And the lower level iterator helpers:

* ``SimpleFeatureIteratorImpl`` - used by ``DefaultFeatureCollection`` to access all contents
* ``FeatureIteratorImpl``
* ``NoContentIterator`` - used to throw an exception on next()
* ``FilteringIterator``
* ``FeatureIteratorIterator``
* ``FeatureReaderFeatureIterator``
* ``FeatureWriterFeatureIterator``

``FeatureReader``
''''''''''''''''''

Low-level implementation support for implementing ``FeatureReader``.

* ``DefaultFeatureReader``
* ``DefaultFIDReader``
* ``EmptyFeatureReader``
* ``EmptyFeatureWriter``

These readers wrap around your simple ``FeatureReader`` and add additional functionality such as filtering:

* ``DelegatingFeatureReader``
* ``DelegatingFeatureWriter``
* ``FIDFeatureReader``
* ``FIDReader``
* ``FilteringFeatureReader``
* ``FilteringFeatureWriter``
* ``MaxFeatureReader``
* ``ReTypeFeatureReader``
* ``ForceCoordinateSystemFeatureReader``
* ``ForceCoordinateSystemFeatureResults``
* ``ReprojectFeatureReader``
* ``ReprojectFeatureResults``

A few even work on iterators:

* ``CollectionFeatureReader``
* ``ForceCoordinateSystemIterator``
* ``ReprojectFeatureIterator``

Where general facilities are available we need ``SimpleFeature`` implementations:

* ``DelegateSimpleFeatureReader``
* ``DelegatingSimpleFeatureWriter``
* ``EmptySimpleFeatureReader``
* ``FilteringSimpleFeatureReader``

AttributeReader
'''''''''''''''

Sub-zero: These are not used in practice they were intended to be used for attribute level operations; in practice everyone works
directly with features. The only place where they are used is with the Shapefile implementation where they are used to "join" the attributes
from the ``shp`` and ``dbf`` files.

* ``AbstractAttributeIO``
* ``AttributeReader``
* ``AttributeWriter``
* ``JoiningAttributeReader``
* ``JoiningAttributeWriter``

Wrappers
''''''''

Wrappers used by ``DataUtilities`` to morph to ``SimpleFeatureSource``, ``SimpleFeatureCollection`` etc...

* ``SimpleFeatureCollectionBridge``
* ``SimpleFeatureLockingBridge``
* ``SimpleFeatureSourceBridge``
* ``SimpleFeatureStoreBridge``

Open Web Services
^^^^^^^^^^^^^^^^^

Some of the data structures used by open web services such as WMS and WFS are defined here.

XML
^^^

Some of the SAX, DOM and Transform classes for handling are defined in ``gt-main``.

Their use for XML and GML handling will be covered in ``gt-xml`` module documentation.

Style
^^^^^

The ``gt-main`` interfaces for ``Style`` are a straight extension of the ``gt-api`` interfaces. Please note these implementations are not threadsafe - extensive copy constructors have been provided if you need to copy these data structures for use in an isolated thread.

Filter
^^^^^^

The Filter classes in ``gt-main`` are deprecated; and have been so since GeoTools 2.3. We are having trouble removing all the existing test cases that depend on these old Filter definitions.
