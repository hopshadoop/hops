package io.hops.devices;

import java.util.Objects;

public class GPU {

  private Device gpuDevice;
  private Device renderNode;

  public GPU(Device gpu, Device renderNode) {
    this.setGpuDevice(gpu);
    this.setRenderNode(renderNode);
  }

  public Device getGpuDevice() {
    return gpuDevice;
  }

  public void setGpuDevice(Device gpu) {
    this.gpuDevice = gpu;
  }

  public Device getRenderNode() {
    return renderNode;
  }

  public void setRenderNode(Device renderNode) {
    this.renderNode = renderNode;
  }

  public String toString() {
    return "GPU Device: " + gpuDevice + " -- Render Node: " + renderNode;
  }

  public int hashCode() {
    return Objects.hash(gpuDevice.getMajorDeviceNumber(), gpuDevice.getMinorDeviceNumber());
  }

  public boolean equals(Object obj) {
    GPU gpu = (GPU) obj;
    if(this.gpuDevice.equals(gpu.getGpuDevice())) {
      if (this.renderNode == null && gpu.getRenderNode() == null) {
        return true;
      } else if (this.renderNode.equals(gpu.getRenderNode())) {
        return true;
      }
    }
      return false;
  }
}
