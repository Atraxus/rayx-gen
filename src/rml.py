import argparse
import os
import sys
import xml.etree.ElementTree as ET
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Randomly perturb worldPosition in an RML file"
        )
    )
    parser.add_argument(
        "input_file",
        help="Path to the input RML file",
    )
    parser.add_argument(
        "-n", "--n-samples",
        type=int,
        default=10000,
        help="Number of samples (default: 10000)",
    )
    parser.add_argument(
        "--n-subdirs",
        type=int,
        default=1,
        help="Number of subdirectories to divide the output files into (default: 1 = no subdivision)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="out",
        help="Output directory (default: ./out)",
    )
    parser.add_argument(
        "--max-tries-mult",
        type=int,
        default=10,
        help="Max tries = n_samples * multiplier (default: 10)",
    )
    return parser.parse_args()


def extract_world_position(tree):
    root = tree.getroot()
    print(f"Parsed XML root: {root.tag} {root.attrib}")

    beam = root.find("beamline")
    if beam is None:
        sys.exit("Error: no beamline element.")

    count = len(beam)
    print(f"Beamline elements: {count}")

    idx = count - 2
    elem = beam[idx]
    print(f"Selected element [{idx}]: {elem.tag} {elem.attrib}")

    wp = elem.find('.//param[@id="worldPosition"]')
    if wp is None:
        sys.exit("Error: no worldPosition param.")

    x, y, z = map(lambda t: float(t.text), wp)
    print(f"Original worldPosition: x={x:.3f}, y={y:.3f}, z={z:.3f}")
    return wp, (x, y, z), idx


def generate_samples(coords, n):
    samples = []
    print(f"Sampling {n} points...")

    for _ in range(n):
        pt = tuple(np.random.uniform(v - 1, v + 1) for v in coords)
        samples.append(pt)

    ivs = ", ".join(f"[{v - 1:.3f}, {v + 1:.3f}]" for v in coords)
    print(f"Intervals: {ivs}")
    print("Sampling done.")
    return samples

def save_samples(tree, wp_elem, samples, out_dir, inp_name, n_subdirs):
    os.makedirs(out_dir, exist_ok=True)
    width = len(str(len(samples) - 1))
    print(f"Saving {len(samples)} samples...")

    os.makedirs(out_dir, exist_ok=True)
    width = len(str(len(samples) - 1))
    print(f"Saving {len(samples)} samples into {n_subdirs} subdirectories...")

    for i, samp in enumerate(samples):
        for j in range(3):
            wp_elem[j].text = f"{samp[j]}"

        subdir_idx = i * n_subdirs // len(samples)
        subdir_path = os.path.join(out_dir, f"subdir_{subdir_idx}")
        os.makedirs(subdir_path, exist_ok=True)

        name = f"{str(i).zfill(width)}_{inp_name}"
        path = os.path.join(subdir_path, name)
        tree.write(path)

        if i % 1000 == 0:
            print(f"  saved {i}")
    print("All done.")


def main():
    args = parse_args()
    print("Starting perturbation...")
    print(f"Input: {args.input_file}")

    tree = ET.parse(args.input_file)
    wp_elem, coords, idx = extract_world_position(tree)
    samples = generate_samples(
        coords, args.n_samples
    )
    save_samples(
        tree, wp_elem, samples,
        args.output_dir, os.path.basename(args.input_file),
        args.n_subdirs
    )


if __name__ == "__main__":
    main()

