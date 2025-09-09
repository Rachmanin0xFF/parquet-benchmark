default_config = {
    "queries": [
        {
            "name": "Tiny bbox",
            "bbox": {
                "type": "uniform",
                "geometry": [-77.024438, 38.898135, -77.022400, 38.900030]
            },
            "display_traffic": True
        },
        {
            "name": "Random bboxes (small)",
            "bbox": {
                "type": "random",
                "size": [0.0001, 0.1]
            },
            "samples": 5
        },
       {
            "name": "Boston",
            "bbox": {
                "type": "uniform",
                "geometry": [-71.161194, 42.297881, -70.941811, 42.433340]
            },
            "display_traffic": True
        },
        {
            "name": "Paris",
            "bbox": {
                "type": "uniform",
                "geometry": [2.106628, 48.701838, 2.580414, 48.969400]
            },
            "display_traffic": True
        }
    ]
}