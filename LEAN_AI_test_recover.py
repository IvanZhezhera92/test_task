#!/usr/bin/env python3

__title__ = "TestTask"
__version__ = "1.0"
__license__ = "None"
__copyright__ = "None"
__category__  = "Product"
__status__ = "In_process"
__author__ = "Ivan Zhezhera"
__maintainer__ = "None"
__email__ = "ivanzhezhera92@gmail.com"
'''
Lib's:
pip install pypylon
pip install pika
pip install numpy
pip install matplotlib
pip3 install opencv-python

docker pull rabbitmq
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.10-management
docker run --name data_base -p 5432:5432 -e POSTGRES_USER=sova -e POSTGRES_PASSWORD=1234 -e POSTGRES_DB=camera_db -d postgres
'''


import pypylon.pylon as py
import numpy as np
import os
import sys
import cv2
import tempfile
import json

from datetime import datetime
import pika
import psycopg2
from psycopg2 import Error

class virtual_camera(object):
    def __init__(self):
        os.environ["PYLON_CAMEMU"] = "1"
        self.cam = py.InstantCamera(py.TlFactory.GetInstance().CreateFirstDevice())
        self.width, self.height = 800, 600
        self.temp_img_dir = tempfile.mkdtemp()
        self.logger_img_dir = "./logger"
        self.json_log_dir = "./json"
        self.json_logger_file = "logger.json"
        self.rabbitMQ_host = 'localhost'
        self.queue = "logger"
        self.user_name = "sova"
        self.password = "1234"
        self.host = "0.0.0.0"
        self.port = "5432"
        self.db_name = "camera_db"
        self.converter = py.ImageFormatConverter()
        #self.converter.OutputPixelFormat = py.PixelType_Mono8packed
        self.converter.OutputBitAlignment = py.OutputBitAlignment_MsbAligned


    def meta_data_generation(self, img : np.int16, file_name : str):
        """
        Meta data generatioin method.
        Input  -> img[cv2 format], file_name[text format]
        Output -> dict with 4 string values or 0 value in case of None image 
        """
        if img is not None:
            
            return {"time_code": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), 
                    "file_name": file_name, 
                    "dimensions": str(img.shape[1]) + str(":") + str(img.shape[0]), 
                    "path":self.temp_img_dir}

        else:
            return 0


    def image_generation(self, quantity = 100):
        """
        Image generation method for emulation of the pylon camera.
        Input  -> quantity[int]
        Output -> no
        """
        test_pattern = np.fromfunction(lambda i, j, k:j % 256, (self.height, self.width, 3), dtype = np.int16)
        for i in range(quantity):
            pattern = np.roll(test_pattern, i, axis=1)
            cv2.imwrite(str(self.temp_img_dir) + "/" + str(i) + ".png", pattern)
            
        print("[INFO] Image generation had finished. All images had saved!")


    def image_resizing(self, img : np.int16, scale_percent = 0.6):
        """
        Image resizing method.
        Input  -> img [np.int16], scale_percent[float] 
        Output -> scaled image[np.int16]
        """
        dim = (int(img.shape[1] * scale_percent), int(img.shape[0] * scale_percent))
        return cv2.resize(img, dim, interpolation = cv2.INTER_AREA)
        

    def image_collector_core(self, img : np.int16, file_name : str):
        """
        Process of image logging. Acts as a hub for logging camera data.
        Dependency -> image_resizing()
                   -> meta_data_generation()
                   -> json_logger()
                   -> rabbitMQ_logger()
        Input  -> img [np.int16], file_name [str]
        Output -> ret [dict] or 0 value in case of None image 
        """
        if img is not None:

            scaled_image = self.image_resizing(img = img)
            ret = self.meta_data_generation(img = img, file_name = file_name)         
            
            self.json_logger(data = json.dumps(ret)) 
            self.rabbitMQ_logger(log_data_str = json.dumps(ret)) if ret != 0 else print("[INFO] RabbitMG has not data for imege:{} and it scaled copy!".format(file_name))
            cv2.imwrite(str(self.logger_img_dir) + "/" + str(file_name), img)
            cv2.imwrite(str(self.logger_img_dir) + "/scaled_" + str(file_name), scaled_image)
            return ret

        else:
            print("[INFO] Image collection error!")
            return 0


    def camera_grab_image(self): 
        """
        Camera grab image method. The method creates the database, 
        emulates the camera, performs minimal camera settings, 
        reads all generated images in the local directory and 
        sends data to the postgres database
        Dependency -> postgres_table_creation()
                   -> postgres_query_insert()
        Input  -> no
        Output -> no 
        """
        try:
            connection = psycopg2.connect(user=self.user_name,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.db_name)

            self.postgres_table_creation(connection = connection)
            self.image_generation()
            self.cam.Open()
            self.cam.ImageFilename = self.temp_img_dir
            # enable image file test pattern
            self.cam.ImageFileMode = "On"
            # disable testpattern [ image file is "real-image"]
            self.cam.TestImageSelector = "Off"
            # choose one pixel format. camera emulation does conversion on the fly
            self.cam.PixelFormat = "Mono8"
            files_quantity = len([entry for entry in os.listdir(self.temp_img_dir) if os.path.isfile(os.path.join(self.temp_img_dir, entry))])
            self.cam.StartGrabbingMax(files_quantity)
            local_counter = 0
            while self.cam.IsGrabbing():
                grabResult = self.cam.RetrieveResult(1000)
                img = self.converter.Convert(grabResult).GetArray()
                file_name = str("IMG_")+str(local_counter)+str(".png")
                data = self.image_collector_core(img = img, file_name = file_name)
                self.postgres_query_insert(img = img, data = data, connection = connection)

                grabResult.Release()
                local_counter = local_counter + 1

            self.cam.StopGrabbing()
            self.cam.Close()
            

        except:
            print(" [INFO] Camera emulation is broken")

        finally:
            if (connection):
                connection.close()
                print(" [INFO] PostgreSQL connection is closed")


    def rabbitMQ_logger(self, log_data_str : str):
        """
        RabbitMQ logger method. Sending data to the RabbitMQ message broker.
        Input  -> log_data_str [str]
        Output -> no
        """
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitMQ_host))
            channel = connection.channel()
            channel.queue_declare(queue = self.queue)
            channel.basic_publish(exchange="", routing_key=self.queue, body=log_data_str)
            print(" [INFO] Sent to rabbitMQ queue: " + log_data_str) 
            connection.close()

        except:
            connection.close()
            print(" [INFO] RabbitMQ connection is broken") 

    def json_logger(self, data : dict):
        """ 
        Json logger method. Makes a copy of the logging data in json file.
        Input  -> data[dict]
        Output -> no
        """
        try:
            with open(self.json_log_dir + str("/") + self.json_logger_file, "a+") as json_logger_file:
                json.dump(data, json_logger_file)

        except:
            print(" [INFO] JSON logging is broken")

    def postgres_table_creation(self, connection):
        """
        Method for database creation.
        input  -> connection
        output -> no
        """
        try:
            # Connect to an existing database
            cursor = connection.cursor()

            create_table_query = """CREATE TABLE camera_src
                  (ID                 INT PRIMARY KEY  NOT NULL,
                  FILE_NAME           VARCHAR (100)    NOT NULL,
                  DATE_TIME           timestamp        NOT NULL,
                  IMAGE_SIZE          TEXT             NOT NULL,
                  IMAGE_PATH          VARCHAR (100)    NOT NULL,
                  IMAGE_SRC           TEXT); """
            
            cursor.execute(create_table_query)
            connection.commit()
            print(" [INFO] Table created successfully in PostgreSQL ")

        except (Exception, Error) as error:
            print(" [INFO] Error while connecting to PostgreSQL", error)

        finally:
            cursor.close()    
            

    def postgres_query_insert(self, img : np.int16, data : dict, connection):
        """
        Method for query insertion for postgres database.
        input  -> img[np.int16], data[dict], connection
        output -> no
        """
        try:

            cursor = connection.cursor()

            id_value = data["file_name"][4:-4]
            img_name = data["file_name"]
            date_time = data["time_code"]
            dimensions = data["dimensions"]
            file_path = str(self.logger_img_dir)
            image_to_string = cv2.imencode('.png', img)[1].tostring()

            # string to image
            #nparr = np.fromstring(image_to_string, np.uint8)
            #img = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)

            insert_query = """ INSERT INTO camera_src (ID, FILE_NAME, DATE_TIME, IMAGE_SIZE, IMAGE_PATH, IMAGE_SRC) VALUES (%s, %s, %s, %s, %s, %s)"""
            item_tuple = (id_value, img_name, date_time, dimensions, file_path, image_to_string)

            cursor.execute(insert_query, item_tuple)

            connection.commit()
            print(" [INFO] sRecord inserted successfully")

        except (Exception, psycopg2.Error) as error:
            print(" [INFO] Error while connecting to PostgreSQL", error)

        finally:
            cursor.close()
                


    
    



if __name__ == "__main__": 

    try:
        VC = virtual_camera()
        VC.camera_grab_image()

    except Exception as e:
        print (e.message, e.args)


    


 





