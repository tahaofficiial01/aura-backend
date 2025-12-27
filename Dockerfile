# Use a lightweight Node.js base image
FROM node:lts-alpine

# Set the working directory
WORKDIR /app

# Copy package.json and install dependencies to leverage Docker layer caching
COPY package*.json ./
RUN npm install

# Copy the rest of your application code
COPY . .

# Expose the port (Hugging Face expects 7860)
EXPOSE 7860

# Define the command to start your application
CMD ["npm", "start"]
