from .version import __version__
import os
import glob
import json
import numpy as np
import io


def draw_muon_parameters(prng, num=32):
    """
    Returns random x, y position and radius in image w.t.t to num (pixel).
    """
    x = prng.uniform(low=0.25 * num, high=0.75 * num)
    y = prng.uniform(low=0.25 * num, high=0.75 * num)
    r = prng.uniform(low=num / 10, high=3 * num / 10)
    return {"x": x, "y": y, "r": r}


def init_image_with_ring(x, y, r, num=32):
    """
    Returns a binary image containing a ring at x, y with radius r.
    """
    TAU = np.pi * 2
    img = np.zeros(shape=(num, num))
    for phi in np.linspace(0, TAU, num * 10):
        xp = x + r * np.cos(phi)
        yp = y + r * np.sin(phi)
        xpp = int(np.round(xp))
        ypp = int(np.round(yp))
        if xpp >= 0 and xpp < num:
            if ypp >= 0 and ypp < num:
                if img[xpp, ypp] == 0:
                    img[xpp, ypp] = 1
    return img


def image_dumps(img):
    """
    Returns an ascii-art image for a human to spectate in a text editor.
    """
    txt = io.StringIO()
    for x in range(img.shape[0]):
        for y in range(img.shape[1]):
            if img[x, y] == 0:
                txt.write("..")
            else:
                txt.write("##")
        txt.write("\n")
    txt.seek(0)
    return txt.read()


def image_loads(txt):
    """
    Returns a binary image from ascii-art string.
    """
    lines = str.splitlines(txt)
    num = len(lines)
    img = np.zeros(shape=(num, num))
    for x in range(num):
        for y in range(num):
            char = lines[x][2 * y]
            if char != ".":
                img[x, y] = 1
    return img


def hough_binning(num):
    """
    Returns estimates for the bins in a hough transformation when looking
    for a ring in an image.
    """
    xs = np.arange(4, num - 3)
    ys = np.arange(4, num - 3)
    rs = np.linspace(num / 10, 3 * num / 10, 16)
    return xs, ys, rs


def hough_transform(img, binning=None):
    """
    Returns the response in (x, y, r) space for rings in an image 'img'
    """
    if binning is None:
        binning = hough_binning(num=img.shape[0])
    xs, ys, rs = binning
    response = np.zeros(shape=(len(xs), len(ys), len(rs)))
    for ix, x in enumerate(xs):
        for iy, y in enumerate(ys):
            for ir, r in enumerate(rs):
                mask = init_image_with_ring(x=x, y=y, r=r, num=img.shape[0])
                response[ix, iy, ir] = np.sum(mask * img)
    return response


def argmax3(m):
    """
    Returns the index of the maximum entry in 3D matrix 'm'.
    """
    imax = 0
    amax = (-1, -1, -1)
    for ix in range(m.shape[0]):
        for iy in range(m.shape[1]):
            for ir in range(m.shape[2]):
                if m[ix, iy, ir] > imax:
                    imax = float(m[ix, iy, ir])
                    amax = (int(ix), int(iy), int(ir))
    return amax


def reconstruct_muon_ring_from_image(img):
    """
    Estimates the most likely ring parameters (x, y, r) from an image 'img'.
    """
    binning = hough_binning(num=img.shape[0])
    response = hough_transform(img=img, binning=binning)
    amax = argmax3(m=response)
    return {
        "x": float(binning[0][amax[0]]),
        "y": float(binning[1][amax[1]]),
        "r": float(binning[2][amax[2]]),
    }


def init(work_dir, num_events=96, seed=1, num_pixel=32):
    """
    Makes a working directory and fills it with images of muons and notes about
    the muon's simulation truth.
    """
    num = num_pixel
    prng = np.random.Generator(np.random.PCG64(seed))

    os.makedirs(work_dir, exist_ok=True)
    for i in range(num_events):
        truth = draw_muon_parameters(prng=prng, num=num)
        opath = os.path.join(work_dir, f"{i:03d}")
        with open(opath + ".truth.json", "wt") as f:
            f.write(json.dumps(truth))
        img = init_image_with_ring(
            x=truth["x"], y=truth["y"], r=truth["r"], num=num
        )
        with open(opath + ".image.txt", "wt") as f:
            f.write(image_dumps(img=img))


def reconstruct_event(work_dir, event_number):
    ipath = os.path.join(work_dir, f"{event_number:03d}")

    with open(ipath + ".image.txt", "rt") as f:
        img = image_loads(f.read())

    recon = reconstruct_muon_ring_from_image(img=img)

    with open(ipath + ".recon.json", "wt") as f:
        f.write(json.dumps(recon))

    return recon


def make_jobs(work_dir):
    """
    For map and reduce using 'run_job()'.
    """
    jobs = []
    for path in glob.glob(os.path.join(work_dir, "*.image.txt")):
        job = {}
        job["work_dir"] = work_dir
        job["event_number"] = int(os.path.basename(path)[0:3])
        jobs.append(job)
    return jobs


def run_job(job):
    """
    Run a single analysis job.
    Opens a muon image, estimates the ring's parameters and writes them next
    to the image.
    """
    return reconstruct_event(**job)


def read_all(work_dir):
    """
    Reads all muon events in the working directory which already have a
    'recon'.
    """
    events = {}
    for path in glob.glob(os.path.join(work_dir, "*.recon.json")):
        event_number = int(os.path.basename(path)[0:3])
        ipath = os.path.join(work_dir, f"{event_number:03d}")

        events[event_number] = {}

        with open(ipath + ".image.txt", "rt") as f:
            img = image_loads(f.read())
        with open(ipath + ".truth.json", "rt") as f:
            truth = json.loads(f.read())
        with open(ipath + ".recon.json", "rt") as f:
            recon = json.loads(f.read())

        events[event_number]["image"] = img
        events[event_number]["truth"] = truth
        events[event_number]["recon"] = recon
    return events


def print_summary(work_dir):
    """
    Prints the true vs. reco. statistics found in a working directory.
    Makes three 2D 'histograms' showing each confusion inx, y, and radius r.
    """
    wd = read_all(work_dir=work_dir)

    nums = []
    x_t = []
    x_r = []
    y_t = []
    y_r = []
    r_t = []
    r_r = []

    for event_number in wd:
        en = event_number
        nums.append(en)
        x_t.append(wd[en]["truth"]["x"])
        y_t.append(wd[en]["truth"]["y"])
        r_t.append(wd[en]["truth"]["r"])
        x_r.append(wd[en]["recon"]["x"])
        y_r.append(wd[en]["recon"]["y"])
        r_r.append(wd[en]["recon"]["r"])

    xs, ys, rs = hough_binning(num=32)

    x_hist = np.histogram2d(x=x_t, y=x_r, bins=(xs, xs))[0]
    y_hist = np.histogram2d(x=y_t, y=y_r, bins=(ys, ys))[0]
    r_hist = np.histogram2d(x=r_t, y=r_r, bins=(rs, rs))[0]

    print("x-position true vs. reco")
    print("------------------------")
    print(image_dumps(np.fliplr(x_hist)))

    print("y-position true vs. reco")
    print("------------------------")
    print(image_dumps(np.fliplr(y_hist)))

    print("radius true vs. reco")
    print("------------------------")
    print(image_dumps(np.fliplr(r_hist)))
