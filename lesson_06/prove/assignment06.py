""" 
Course: CSE 351
Assignment: 06
Author: [Zac Volmer]

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 :
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def process_images_in_folder(input_folder,              # input folder with images
                             output_folder,             # output folder for processed images
                             processing_function,       # function to process the image (ie., task_...())
                             load_args=None,            # Optional args for cv2.imread
                             processing_args=None):     # Optional args for processing function

    create_folder_if_not_exists(output_folder)
    print(f"\nProcessing images from '{input_folder}' to '{output_folder}'...")

    processed_count = 0
    for filename in os.listdir(input_folder):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue

        input_image_path = os.path.join(input_folder, filename)
        output_image_path = os.path.join(output_folder, filename)

        try:
            if load_args is not None:
                img = cv2.imread(input_image_path, load_args)
            else:
                img = cv2.imread(input_image_path)

            if img is None:
                print(f"Warning: Could not read image '{input_image_path}'. Skipping.")
                continue

            if processing_args:
                processed_img = processing_function(img, *processing_args)
            else:
                processed_img = processing_function(img)

            cv2.imwrite(output_image_path, processed_img)

            processed_count += 1
        except Exception as e:
            print(f"Error processing file '{input_image_path}': {e}")

    print(f"Finished processing. {processed_count} images processed into '{output_folder}'.")

# ---------------------------------------------------------------------------
def smooth_worker(in_q, out_q):
    while True:
        item = in_q.get()
        if item is None:
            break
        filename, img = item
        try:
            sm = task_smooth_image(img, GAUSSIAN_BLUR_KERNEL_SIZE)
            out_q.put((filename, sm))
        except Exception as e:
            print(f"Error in smoothing '{filename}': {e}")

def grayscale_worker(in_q, out_q):
    while True:
        item = in_q.get()
        if item is None:
            break
        filename, img = item
        try:
            gray = task_convert_to_grayscale(img)
            out_q.put((filename, gray))
        except Exception as e:
            print(f"Error in grayscale '{filename}': {e}")

def edge_worker(in_q):
    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)
    while True:
        item = in_q.get()
        if item is None:
            break
        filename, img = item
        try:
            edges = task_detect_edges(img, CANNY_THRESHOLD1, CANNY_THRESHOLD2)
            cv2.imwrite(os.path.join(STEP3_OUTPUT_FOLDER, filename), edges)
        except Exception as e:
            print(f"Error in edge detect '{filename}': {e}")

# ---------------------------------------------------------------------------
def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    create_folder_if_not_exists(STEP1_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP2_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)

    cpu_count = max(1, mp.cpu_count())
    num_smoothers = max(1, cpu_count // 3)
    num_grays = max(1, cpu_count // 3)
    num_edgers = max(1, cpu_count - (num_smoothers + num_grays))
    if num_edgers < 1:
        num_edgers = 1

    queue1 = mp.Queue(maxsize=10)
    queue2 = mp.Queue(maxsize=10)
    queue3 = mp.Queue(maxsize=10)

    smoothers = []
    for _ in range(num_smoothers):
        p = mp.Process(target=smooth_worker, args=(queue1, queue2))
        p.start()
        smoothers.append(p)

    grays = []
    for _ in range(num_grays):
        p = mp.Process(target=grayscale_worker, args=(queue2, queue3))
        p.start()
        grays.append(p)

    edgers = []
    for _ in range(num_edgers):
        p = mp.Process(target=edge_worker, args=(queue3,))
        p.start()
        edgers.append(p)

    print(f"\nLoading images from '{INPUT_FOLDER}' into pipeline...")
    total = 0
    for filename in os.listdir(INPUT_FOLDER):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue
        path = os.path.join(INPUT_FOLDER, filename)
        img = cv2.imread(path)
        if img is None:
            print(f"Warning: Could not read image '{path}'. Skipping.")
            continue
        queue1.put((filename, img))
        total += 1

    print(f"Finished loading. {total} images put into pipeline.")

    for _ in range(num_smoothers):
        queue1.put(None)

    for p in smoothers:
        p.join()

    for _ in range(num_grays):
        queue2.put(None)

    for p in grays:
        p.join()

    for _ in range(num_edgers):
        queue3.put(None)

    for p in edgers:
        p.join()

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Grayscale images are in: '{STEP1_OUTPUT_FOLDER}'")
    print(f"Smoothed images are in: '{STEP2_OUTPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')
