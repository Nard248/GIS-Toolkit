from ast import While
import os
import sys
from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsProject,
    QgsRasterLayer,
    QgsVectorLayer,
    QgsCoordinateReferenceSystem,
    QgsApplication
)
import os
import sys
from qgis.core import QgsApplication, QgsPrintLayout, QgsLayoutItemMap
from qgis.PyQt.QtGui import QColor
from qgis.core import QgsProject, QgsSymbol, QgsRendererCategory, QgsCategorizedSymbolRenderer, QgsRasterLayer, \
    QgsVectorLayer, QgsCoordinateReferenceSystem, QgsVectorFileWriter
from qgis.gui import (
    QgsMapCanvas
)


def read_layer(layer_path, name_inside_QGIS, crs):
    if name_inside_QGIS == 'OpenStreetMap':

        readed_layer = QgsRasterLayer(layer_path, name_inside_QGIS, 'wms')

    else:
        readed_layer = QgsVectorLayer(layer_path, name_inside_QGIS, "ogr")

    if not readed_layer.isValid():
        name = os.path.basename(layer_path)
        print(f"{name} Layer failed to load!")

    else:
        QgsProject.instance().addMapLayer(readed_layer)

    if name_inside_QGIS != 'OpenStreetMap':
        readed_layer.setCrs(QgsCoordinateReferenceSystem(crs))

    return readed_layer


def RemoveAllLayersExcept(*layers):
    layer_ids = []

    for l in layers:
        layer_ids.append(l.id())

    print('layer_ids', layer_ids)

    for lyr in QgsProject.instance().mapLayers():

        print(lyr)

        if lyr not in layer_ids:

            print('not')

            QgsProject.instance().removeMapLayer(lyr)

        else:

            pass


if "__main__" == __name__:

    name_of_project = sys.argv[1]
    state_name = sys.argv[2]
    # locations
    unserved_unfunded_in_fp = sys.argv[3]  # ""
    unserved_unfunded_10 = sys.argv[4]
    unserved_unfunded_30 = sys.argv[5]
    # polygons
    state_polygon = sys.argv[6]
    county_polygon = sys.argv[7]
    cb_footprint = sys.argv[8]
    cb_footprint_10 = sys.argv[9]
    cb_footprint_30 = sys.argv[10]
    provider_name = sys.argv[11]

    # save_path
    project_path = sys.argv[12]

    QGISAPP = QgsApplication([], True)
    QgsApplication.initQgis()

    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

    # Adds OpenStreetMap Layer
    urlWithParams = 'type=xyz&url=https://tile.openstreetmap.org/%7Bz%7D/%7Bx%7D/%7By%7D.png&zmax=19&zmin=0'
    rlayer2 = read_layer(urlWithParams, 'OpenStreetMap', 'wms')

    if county_polygon != '':
        county_polygon_layer = read_layer(county_polygon, f"{state_name} County Outline", "EPSG:4326")
        # county_polygon_layer.renderer().symbol().setColor(QColor("#3579b1"))
        county_polygon_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\county outline style .qml')

    if state_polygon != '':
        state_polygon_layer = read_layer(state_polygon, f"{state_name} State Outline", "EPSG:4326")
        # state_polygon_layer.renderer().symbol().setColor(QColor("#0156ff"))
        state_polygon_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\state outline style.qml')

    if cb_footprint != '':
        cb_footprint_layer = read_layer(cb_footprint, f"{provider_name} CB Footprint", "EPSG:4326")
        # cb_footprint_layer.renderer().symbol().setColor(QColor("#ff01fb"))
        cb_footprint_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\CB Footprint style.qml')

    if cb_footprint_10 != '':
        cb_footprint_10_layer = read_layer(cb_footprint_10, f"{provider_name} CB Footprint 10 Miles Buffer Ring",
                                           "EPSG:4326")
        # cb_footprint_10_layer.renderer().symbol().setColor(QColor("#01ff34"))
        cb_footprint_10_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\CB Footprint 10 Miles Buffer Ring style.qml')

    if cb_footprint_30 != '':
        cb_footprint_30_layer = read_layer(cb_footprint_30, f"{provider_name} CB Footprint 30 Miles Buffer Ring",
                                           "EPSG:4326")
        # cb_footprint_30_layer.renderer().symbol().setColor(QColor("#fe78a4"))
        cb_footprint_30_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\CB Footprint 30 Miles Buffer Ring style.qml')

    if unserved_unfunded_in_fp != '':
        unserved_unfunded_in_fp_layer = read_layer(unserved_unfunded_in_fp, "Unserved & Unfunded Locations in FP",
                                                   "EPSG:4326")
        # unserved_unfunded_in_fp_layer.renderer().symbol().setColor(QColor("#ff6201"))
        unserved_unfunded_in_fp_layer.loadNamedStyle(
            r'C:\OSGeo4W\processing_utilities\Styles\Unserved and Unfunded Locations in FP style.qml')

    if unserved_unfunded_10 != '':
        unserved_unfunded_10_layer = read_layer(unserved_unfunded_10,
                                                "Unserved & Unfunded Locations in 10 Miles Buffer Ring", "EPSG:4326")
        # unserved_unfunded_10_layer.renderer().symbol().setColor(QColor("#ff9501"))
        unserved_unfunded_10_layer.loadNamedStyle(r'C:\OSGeo4W\processing_utilities\Styles\Unserved and Unfunded Locations in 10 Miles Buffer style.qml')



    if unserved_unfunded_30 != '':
        unserved_unfunded_30_layer = read_layer(unserved_unfunded_30,
                                                "Unserved & Unfunded Locations in 30 Miles Buffer Ring", "EPSG:4326")
        # unserved_unfunded_30_layer.renderer().symbol().setColor(QColor("#ffd001"))
        unserved_unfunded_30_layer.loadNamedStyle(r'C:\OSGeo4W\processing_utilities\Styles\Unserved and Unfunded Locations in 30 Miles Buffer Ring style.qml')


    # Saves qgz file
    file = (project_path + "/" + name_of_project + " Project" + ".qgz")
    layout = QgsPrintLayout(project)
    map = QgsLayoutItemMap(layout)
    canvas = QgsMapCanvas()
    # set extent to the extent of our layer
    canvas.setExtent(state_polygon_layer.extent())
    # set the map canvas layer set
    canvas.setLayers([state_polygon_layer])

    project.write(file)
