import tutorial_for_pypoolparty as tut
import numpy as np


def test_imgage_io():
    num = 32
    prng = np.random.Generator(np.random.PCG64(5))
    for i in range(100):
        muo = tut.draw_muon_parameters(prng=prng, num=num)
        img = tut.init_image_with_ring(**muo, num=num)
        txt = tut.image_dumps(img=img)
        imb = tut.image_loads(txt=txt)
        np.testing.assert_almost_equal(img, imb)
