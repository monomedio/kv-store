//
// Created by Sam Hui on 2023-12-03.
//
#include "../src/database.hh"

int main() {

    Database db(256, 0, 10);
    db.setBufferpoolEnabled(false);
    db.Open("ssts/bloom_bsst_test", BSST);

    for (int i = 0; i < 256; i++) {
        db.Put(i + 1, i + 1);
    }

    db.Close();

    return 0;
}